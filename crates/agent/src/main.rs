//! eo-agent: read a workspace, plan build/run commands, submit to the cluster
//! via the node IPC, then analyze the result. A thin deterministic workflow
//! with two optional LLM steps (plan, analyze).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::{json as jmacro, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const MAX_FILES: usize = 200;
const MAX_DEPTH: usize = 3;

#[derive(Parser, Debug)]
#[command(
    name = "eo-agent",
    about = "Plan, submit and analyze a project on the cluster"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Scan a workspace, plan build/run, submit to the cluster and analyze.
    Run(RunArgs),
}

#[derive(clap::Args, Debug)]
struct RunArgs {
    /// Workspace directory to build and run (resolved on the node host).
    #[arg(long)]
    workspace: PathBuf,
    /// Node IPC Unix socket path (must be a node on this same host).
    #[arg(long, default_value = "~/.edge-orchestrator/ipc.sock")]
    socket: String,
    /// Natural-language goal (optional).
    #[arg(long)]
    goal: Option<String>,
    /// Pin execution to a specific executor node id (optional).
    #[arg(long)]
    target_node: Option<String>,
    /// Max attempts (LLM may revise commands after a failure).
    #[arg(long, default_value_t = 2)]
    max_attempts: u32,
    /// Emit machine-readable JSON only.
    #[arg(long)]
    json: bool,
    /// Print the plan and exit without submitting.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Clone)]
struct WorkspaceInfo {
    root: PathBuf,
    files: Vec<String>,
}

#[derive(Debug, Clone)]
struct Plan {
    build_cmd: Vec<String>,
    run_cmd: Vec<String>,
    work_dir: String,
}

fn scan_workspace(root: &Path) -> Result<WorkspaceInfo> {
    let mut files = Vec::new();
    walk(root, root, 0, &mut files)?;
    files.sort();
    Ok(WorkspaceInfo {
        root: root.to_path_buf(),
        files,
    })
}

fn walk(root: &Path, dir: &Path, depth: usize, out: &mut Vec<String>) -> Result<()> {
    if depth > MAX_DEPTH || out.len() >= MAX_FILES {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name == ".git" || name == "target" || name == "node_modules" {
            continue;
        }
        if path.is_dir() {
            walk(root, &path, depth + 1, out)?;
        } else if let Ok(rel) = path.strip_prefix(root) {
            out.push(rel.to_string_lossy().to_string());
        }
    }
    Ok(())
}
fn has(info: &WorkspaceInfo, name: &str) -> bool {
    info.files
        .iter()
        .any(|f| f == name || f.ends_with(&format!("/{name}")))
}

fn first_with_ext(info: &WorkspaceInfo, ext: &str) -> Option<String> {
    info.files.iter().find(|f| f.ends_with(ext)).cloned()
}

fn ensure_tool(pkg: &str) -> String {
    format!(
        "(command -v {pkg} >/dev/null 2>&1 || (DEBIAN_FRONTEND=noninteractive apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq {pkg}))"
    )
}

/// Deterministic planner for common project layouts (no LLM needed).
fn heuristic_plan(info: &WorkspaceInfo) -> Plan {
    let work = "/root/project".to_string();
    if has(info, "Cargo.toml") {
        return Plan {
            build_cmd: vec![format!("{} && cargo build --release", ensure_tool("cargo"))],
            run_cmd: vec!["cargo run --release".into()],
            work_dir: work,
        };
    }
    let top_cs: Vec<&String> = info
        .files
        .iter()
        .filter(|f| !f.contains('/') && f.ends_with(".c"))
        .collect();
    if top_cs.len() == 1 {
        let cfile = top_cs[0];
        return Plan {
            build_cmd: vec![format!("{} && gcc {} -o app", ensure_tool("gcc"), cfile)],
            run_cmd: vec!["./app".into()],
            work_dir: work,
        };
    }
    if has(info, "Makefile") {
        return Plan {
            build_cmd: vec![format!("{} && make", ensure_tool("make"))],
            run_cmd: vec!["./app".into()],
            work_dir: work,
        };
    }
    if let Some(py) = first_with_ext(info, ".py") {
        return Plan {
            build_cmd: Vec::new(),
            run_cmd: vec![format!("{} && python3 {py}", ensure_tool("python3"))],
            work_dir: work,
        };
    }
    if has(info, "package.json") {
        return Plan {
            build_cmd: vec![format!("{} && npm install", ensure_tool("npm"))],
            run_cmd: vec!["node index.js".into()],
            work_dir: work,
        };
    }
    Plan {
        build_cmd: Vec::new(),
        run_cmd: vec!["ls -la".into()],
        work_dir: work,
    }
}

/// Resolved LLM settings (OpenAI-compatible).
#[derive(Debug, Clone)]
struct LlmConfig {
    base_url: String,
    api_key: String,
    model: String,
}

/// Optional config file: KEY=VALUE lines (like a .env).
fn config_path() -> PathBuf {
    if let Ok(p) = std::env::var("EO_AGENT_CONFIG") {
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".config/eo-agent/config.env")
}

#[cfg(unix)]
fn warn_if_public(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            eprintln!(
                "[eo-agent] warning: {} is readable by others (mode {:o}); run: chmod 600 {}",
                path.display(),
                mode & 0o777,
                path.display()
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_public(_path: &Path) {}

fn read_config_file() -> HashMap<String, String> {
    let path = config_path();
    let mut map = HashMap::new();
    let Ok(content) = std::fs::read_to_string(&path) else {
        return map;
    };
    warn_if_public(&path);
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            map.insert(k.trim().to_string(), v.trim().trim_matches('"').to_string());
        }
    }
    map
}

/// Env var (or alias) first, then the config file.
fn pick(file: &HashMap<String, String>, key: &str, aliases: &[&str]) -> Option<String> {
    if let Ok(v) = std::env::var(key) {
        if !v.is_empty() {
            return Some(v);
        }
    }
    for a in aliases {
        if let Ok(v) = std::env::var(a) {
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    if let Some(v) = file.get(key) {
        if !v.is_empty() {
            return Some(v.clone());
        }
    }
    for a in aliases {
        if let Some(v) = file.get(*a) {
            if !v.is_empty() {
                return Some(v.clone());
            }
        }
    }
    None
}

fn llm_config() -> Option<LlmConfig> {
    let file = read_config_file();
    let base_url = pick(&file, "EO_LLM_BASE_URL", &["OPENAI_BASE_URL"])?;
    let api_key = pick(&file, "EO_LLM_API_KEY", &["OPENAI_API_KEY"]).unwrap_or_default();
    let model =
        pick(&file, "EO_LLM_MODEL", &["OPENAI_MODEL"]).unwrap_or_else(|| "gpt-4o-mini".to_string());
    Some(LlmConfig {
        base_url,
        api_key,
        model,
    })
}

fn llm_enabled() -> bool {
    llm_config().is_some()
}

/// Minimal OpenAI-compatible chat call via curl (no extra HTTP dependency).
/// The API key is only ever passed via the curl argument vector, never logged.
fn llm_chat(system: &str, user: &str) -> Result<String> {
    let cfg =
        llm_config().context("LLM not configured (set EO_LLM_BASE_URL or use a config file)")?;
    let body = jmacro!({
        "model": cfg.model,
        "temperature": 0,
        "messages": [
            { "role": "system", "content": system },
            { "role": "user", "content": user }
        ]
    })
    .to_string();
    let url = format!("{}/chat/completions", cfg.base_url.trim_end_matches('/'));
    let mut args: Vec<String> = vec![
        "-sS".into(),
        "-X".into(),
        "POST".into(),
        url,
        "-H".into(),
        "Content-Type: application/json".into(),
    ];
    if !cfg.api_key.is_empty() {
        args.push("-H".into());
        args.push(format!("Authorization: Bearer {}", cfg.api_key));
    }
    args.push("-d".into());
    args.push(body);
    let out = Command::new("curl")
        .args(&args)
        .output()
        .context("failed to run curl")?;
    if !out.status.success() {
        anyhow::bail!("curl failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    let v: Value = serde_json::from_slice(&out.stdout).context("LLM response not JSON")?;
    let content = v["choices"][0]["message"]["content"]
        .as_str()
        .context("LLM response missing content")?;
    Ok(content.to_string())
}
fn expand_tilde(p: &str) -> String {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    p.to_string()
}

async fn rpc(socket: &str, method: &str, params: Value) -> Result<Value> {
    let path = expand_tilde(socket);
    let stream = UnixStream::connect(&path)
        .await
        .with_context(|| format!("connect {path}"))?;
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let req = jmacro!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": uuid::Uuid::new_v4().to_string()
    });
    let mut payload = req.to_string();
    payload.push(char::from(10));
    w.write_all(payload.as_bytes()).await?;
    w.flush().await?;
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let v: Value = serde_json::from_str(line.trim()).with_context(|| "invalid RPC response")?;
    if let Some(err) = v.get("error") {
        anyhow::bail!("RPC {method} failed: {err}");
    }
    Ok(v.get("result").cloned().unwrap_or(Value::Null))
}

async fn submit_and_wait(args: &RunArgs, info: &WorkspaceInfo, plan: &Plan) -> Result<Value> {
    let params = jmacro!({
        "project_dir": info.root.to_string_lossy(),
        "work_dir": plan.work_dir.clone(),
        "build_cmd": plan.build_cmd.clone(),
        "run_cmd": plan.run_cmd.clone(),
        "timeout_ms": 300000,
        "target_node": args.target_node.clone(),
    });
    let res = rpc(&args.socket, "submit_project", params).await?;
    let task_id = res["task_id"]
        .as_str()
        .context("submit_project returned no task_id")?
        .to_string();
    eprintln!(
        "[eo-agent] submitted task {task_id}; waiting (cold VM boot + toolchain install can take ~1-2 min)"
    );
    for i in 0..600 {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let r = rpc(
            &args.socket,
            "fetch_project_result",
            jmacro!({ "task_id": task_id }),
        )
        .await?;
        match r["status"].as_str().unwrap_or("pending") {
            "completed" => return Ok(r),
            // The node now reports terminal failures explicitly (unknown task,
            // no reachable executor, no result within the protocol window).
            // Bail out with the reason instead of polling to the 30-min deadline.
            "failed" => {
                anyhow::bail!(
                    "task {task_id} failed on the cluster: {}",
                    r["error"].as_str().unwrap_or("<no reason given>")
                );
            }
            other => {
                if i % 5 == 4 && !args.json {
                    eprintln!(
                        "[eo-agent] still running... {}s (status={other})",
                        (i + 1) * 3
                    );
                }
            }
        }
    }
    anyhow::bail!(
        "timed out after 30 min waiting for project result of task {task_id}; \
         check the node logs of the executing node (look for 'project task ... accepted')"
    )
}

fn decode(b64: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .map(|b| String::from_utf8_lossy(&b).to_string())
        .unwrap_or_default()
}

fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

fn heuristic_analysis(result: &Value) -> String {
    let code = result["exit_code"].as_i64().unwrap_or(-1);
    let stderr = decode(result["stderr"].as_str().unwrap_or(""));
    let stdout = decode(result["stdout"].as_str().unwrap_or(""));
    if code == 0 {
        format!(
            "success (exit 0). last output:
{}",
            tail(&stdout, 5)
        )
    } else {
        format!(
            "failure (exit {code}). stderr tail:
{}",
            tail(&stderr, 15)
        )
    }
}
fn extract_json(s: &str) -> &str {
    let t = s.trim();
    if let (Some(a), Some(b)) = (t.find('{'), t.rfind('}')) {
        if b > a {
            return &t[a..=b];
        }
    }
    t
}

fn parse_plan(content: &str, info: &WorkspaceInfo) -> Result<Plan> {
    let v: Value = serde_json::from_str(extract_json(content)).context("plan is not JSON")?;
    let to_vec = |k: &str| -> Vec<String> {
        v[k].as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    let work_dir = v["work_dir"]
        .as_str()
        .unwrap_or("/root/project")
        .to_string();
    let plan = Plan {
        build_cmd: to_vec("build_cmd"),
        run_cmd: to_vec("run_cmd"),
        work_dir,
    };
    if plan.run_cmd.is_empty() {
        anyhow::bail!("plan has empty run_cmd (files: {})", info.files.len());
    }
    Ok(plan)
}

const PLAN_SYSTEM: &str = "You are a build/run planner for a sandboxed Linux VM (fresh Debian, no toolchain preinstalled). You MUST install missing tools inside the build command, for example apt-get update and apt-get install -y gcc. Return ONLY a JSON object with keys build_cmd (string array), run_cmd (string array), work_dir (usually /root/project).";

fn llm_plan(info: &WorkspaceInfo, goal: Option<&str>) -> Result<Plan> {
    let user = format!(
        "Workspace files:\n{}\n\nGoal: {}\n\nReturn the JSON now.",
        info.files.join("\n"),
        goal.unwrap_or("build and run the project")
    );
    let content = llm_chat(PLAN_SYSTEM, &user)?;
    parse_plan(&content, info)
}

fn llm_revise(
    info: &WorkspaceInfo,
    prev: &Plan,
    result: &Value,
    goal: Option<&str>,
) -> Result<Plan> {
    let system = "A previous build/run attempt failed in a sandboxed Linux VM. Propose corrected commands. Return ONLY a JSON object with keys build_cmd (string array), run_cmd (string array), work_dir.";
    let user = format!( "Files:\n{}\n\nGoal: {}\nPrevious build: {:?}\nPrevious run: {:?}\nExit: {}\nstderr tail:\n{}", info.files.join("\n"), goal.unwrap_or("build and run the project"), prev.build_cmd, prev.run_cmd, result["exit_code"].as_i64().unwrap_or(-1), tail(&decode(result["stderr"].as_str().unwrap_or("")), 20));
    let content = llm_chat(system, &user)?;
    parse_plan(&content, info)
}

fn llm_analysis(result: &Value) -> Result<String> {
    let system =
        "Explain the sandbox execution result in 2-3 sentences and state whether it succeeded.";
    let user = format!(
        "exit_code: {}\nstdout tail:\n{}\nstderr tail:\n{}",
        result["exit_code"].as_i64().unwrap_or(-1),
        tail(&decode(result["stdout"].as_str().unwrap_or("")), 30),
        tail(&decode(result["stderr"].as_str().unwrap_or("")), 30)
    );
    llm_chat(system, &user)
}
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run(args) => run(args).await,
    }
}

async fn run(args: RunArgs) -> Result<()> {
    // Resolve the workspace absolutely so the node resolves the same directory
    // (agent and node share a host in this design).
    let workspace = std::fs::canonicalize(&args.workspace)
        .with_context(|| format!("workspace not found: {}", args.workspace.display()))?;
    if !workspace.is_dir() {
        anyhow::bail!("workspace is not a directory: {}", workspace.display());
    }
    let info = scan_workspace(&workspace)?;

    match llm_config() {
        Some(c) => eprintln!(
            "[eo-agent] LLM: model={} base={} key={}",
            c.model,
            c.base_url,
            if c.api_key.is_empty() { "none" } else { "set" }
        ),
        None => eprintln!("[eo-agent] LLM: disabled (heuristic planner)"),
    }

    let mut plan = if llm_enabled() {
        match llm_plan(&info, args.goal.as_deref()) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("[eo-agent] LLM plan failed: {e}; falling back to heuristic");
                heuristic_plan(&info)
            }
        }
    } else {
        heuristic_plan(&info)
    };

    if args.dry_run {
        println!(
            "[eo-agent] plan: build={:?} run={:?} work_dir={}",
            plan.build_cmd, plan.run_cmd, plan.work_dir
        );
        return Ok(());
    }

    let attempts = args.max_attempts.max(1);
    let mut last = Value::Null;

    for attempt in 1..=attempts {
        if !args.json {
            println!(
                "[eo-agent] attempt {attempt}/{attempts}: build={:?} run={:?}",
                plan.build_cmd, plan.run_cmd
            );
        }
        let result = submit_and_wait(&args, &info, &plan).await?;
        let analysis = if llm_enabled() {
            llm_analysis(&result).unwrap_or_else(|_| heuristic_analysis(&result))
        } else {
            heuristic_analysis(&result)
        };
        if !args.json {
            println!("[eo-agent] analysis: {analysis}");
        }
        let code = result["exit_code"].as_i64().unwrap_or(-1);
        last = result;
        if code == 0 {
            break;
        }
        if attempt == attempts {
            break;
        }
        if llm_enabled() {
            match llm_revise(&info, &plan, &last, args.goal.as_deref()) {
                Ok(p) => plan = p,
                Err(e) => {
                    eprintln!("[eo-agent] revise failed: {e}");
                    break;
                }
            }
        } else {
            break;
        }
    }

    if args.json {
        println!("{}", serde_json::to_string(&last)?);
    }
    if last["exit_code"].as_i64().unwrap_or(-1) != 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(files: &[&str]) -> WorkspaceInfo {
        WorkspaceInfo {
            root: PathBuf::from("/tmp/x"),
            files: files.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn extract_json_takes_object_slice() {
        let s = "noise { 1 } trailing";
        assert_eq!(extract_json(s), "{ 1 }");
    }

    #[test]
    fn heuristic_c_single_file() {
        let p = heuristic_plan(&info(&["main.c"]));
        assert!(p.build_cmd[0].contains("gcc"));
        assert_eq!(p.run_cmd, vec!["./app".to_string()]);
    }

    #[test]
    fn heuristic_cargo_project() {
        let p = heuristic_plan(&info(&["Cargo.toml", "src/main.rs"]));
        assert!(p.build_cmd[0].contains("cargo build"));
    }

    #[test]
    fn heuristic_makefile() {
        let p = heuristic_plan(&info(&["Makefile", "a.c", "b.c"]));
        assert!(p.build_cmd[0].contains("make"));
    }
    #[tokio::test]
    async fn submit_and_wait_roundtrip_against_mock_node() {
        use tokio::net::UnixListener;
        let short = uuid::Uuid::new_v4().simple().to_string();
        let dir = PathBuf::from(format!("/tmp/eo-it-{}", &short[..8]));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("ipc.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        let server = tokio::spawn(async move {
            let mut polls = 0u32;
            while let Ok((stream, _)) = listener.accept().await {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                    continue;
                }
                let req: Value = serde_json::from_str(line.trim()).unwrap();
                let method = req["method"].as_str().unwrap_or("").to_string();
                let id = req["id"].clone();
                let result = match method.as_str() {
                    "submit_project" => {
                        jmacro!({ "task_id": "11111111-1111-1111-1111-111111111111" })
                    }
                    "fetch_project_result" => {
                        polls += 1;
                        if polls < 2 {
                            jmacro!({ "status": "pending" })
                        } else {
                            use base64::Engine;
                            let out =
                                base64::engine::general_purpose::STANDARD.encode("project-hello\n");
                            jmacro!({
                                "status": "completed",
                                "exit_code": 0,
                                "stdout": out,
                                "stderr": "",
                                "execution_time_ms": 1,
                                "executed_on": "22222222-2222-2222-2222-222222222222"
                            })
                        }
                    }
                    _ => Value::Null,
                };
                let resp = jmacro!({ "jsonrpc": "2.0", "result": result, "id": id });
                let mut payload = resp.to_string();
                payload.push(char::from(10));
                let mut w = reader.into_inner();
                w.write_all(payload.as_bytes()).await.unwrap();
                w.flush().await.unwrap();
            }
        });

        let args = RunArgs {
            workspace: PathBuf::from("."),
            socket: sock.to_string_lossy().to_string(),
            goal: None,
            target_node: None,
            max_attempts: 1,
            json: true,
            dry_run: false,
        };
        let info = WorkspaceInfo {
            root: PathBuf::from("."),
            files: Vec::new(),
        };
        let plan = Plan {
            build_cmd: Vec::new(),
            run_cmd: vec!["true".to_string()],
            work_dir: "/root/project".to_string(),
        };
        let res = submit_and_wait(&args, &info, &plan).await.unwrap();
        assert_eq!(res["status"].as_str(), Some("completed"));
        assert_eq!(decode(res["stdout"].as_str().unwrap()), "project-hello\n");
        server.abort();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
