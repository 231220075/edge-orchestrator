// qlean-upload-demo: verify upload(dir, parent) lands at work_dir exactly.
use anyhow::{Result, bail};
use qlean::{Image, ImageConfig, MachineConfig, with_machine};

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("FAILED: {:#}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let image = Image::new(ImageConfig::default()).await?;
    let config = MachineConfig::default();

    with_machine(&image, &config, |vm| {
        Box::pin(async {
            // upload testproj dir into /root, expect it to land at /root/testproj
            vm.upload("./testproj", "/root").await?;
            let check = vm.exec("ls /root/testproj/hello.txt").await?;
            if !check.status.success() {
                bail!("upload did not land at /root/testproj");
            }
            let out = vm.exec("cat /root/testproj/hello.txt").await?;
            println!("content={}", String::from_utf8_lossy(&out.stdout));
            if out.stdout.trim_ascii_end() == b"hello-upload" {
                println!("RESULT: PASS");
            } else {
                bail!("content mismatch");
            }
            Ok(())
        })
    }).await
}
