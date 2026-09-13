// Submit a project directory to the cluster via UDS and poll for the result.
// Usage: submit_project <socket> <project_dir> ["build cmd"] ["run cmd"]
use serde_json::{json as jmacro, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

async fn call(sock: &str, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(sock).await.unwrap();
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
    w.write_all(payload.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    serde_json::from_str(line.trim()).unwrap()
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let sock = args
        .next()
        .expect("usage: submit_project <socket> <project_dir>");
    let dir = args.next().expect("project_dir required");
    let build: Vec<String> = args
        .next()
        .map(|s| s.split(char::from(32)).map(String::from).collect())
        .unwrap_or_default();
    let run: Vec<String> = args
        .next()
        .map(|s| s.split(char::from(32)).map(String::from).collect())
        .unwrap_or_default();

    let resp = call(
        &sock,
        "submit_project",
        jmacro!({
            "project_dir": dir,
            "work_dir": "/root/project",
            "build_cmd": build,
            "run_cmd": run,
            "timeout_ms": 300000
        }),
    )
    .await;
    println!("submit -> {}", resp);
    let task_id = resp["result"]["task_id"].as_str().unwrap_or("").to_string();
    if task_id.is_empty() {
        std::process::exit(1);
    }

    for i in 0..600 {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let r = call(
            &sock,
            "fetch_project_result",
            jmacro!({ "task_id": task_id }),
        )
        .await;
        if r["result"]["status"] == "completed" {
            println!("result -> {}", r);
            return;
        }
        if i % 10 == 0 {
            eprintln!("... still pending ({}s)", (i + 1) * 3);
        }
    }
    eprintln!("timeout waiting for project result");
    std::process::exit(1);
}
