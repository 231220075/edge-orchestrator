use serde_json::json as jmacro;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[tokio::main]
async fn main() {
    let sock = std::env::args()
        .nth(1)
        .expect("usage: submit_task <socket>");
    let wasm =
        wat::parse_str(r#"(module (func $_start) (export "_start" (func $_start)))"#).unwrap();
    let code_b64 = {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(&wasm)
    };
    let id = uuid::Uuid::new_v4().to_string();
    let stream = UnixStream::connect(&sock).await.unwrap();
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);
    let req = jmacro!({
        "jsonrpc": "2.0",
        "method": "submit_to_cas_and_raft",
        "params": {
            "code": code_b64,
            "required_runtime": "Wasm",
            "routing": "AnyExecutor",
            "timeout_ms": 30000
        },
        "id": id
    });
    let mut payload = String::new();
    payload.push_str(&req.to_string());
    payload.push('\n');
    w.write_all(payload.as_bytes()).await.unwrap();
    w.flush().await.unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    println!("{}", line.trim());
}
