// qlean-demo: prove qlean can boot a KVM VM, compile C and run it.
use anyhow::{Result, bail};
use qlean::{Image, ImageConfig, MachineConfig, with_machine};

#[tokio::main]
async fn main() {
    let r = run().await;
    if let Err(e) = r {
        eprintln!("DEMO FAILED: {:#}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    println!("STEP 1: loading default Debian cloud image (first run downloads)...");
    let image = Image::new(ImageConfig::default()).await?;
    let config = MachineConfig::default();

    with_machine(&image, &config, |vm| {
        Box::pin(async {
            println!("STEP 2: VM booted, checking gcc...");
            let has = vm.exec("command -v gcc").await?;
            if !has.status.success() {
                println!("  gcc missing, installing (first time only, may take a while)...");
                let inst = vm.exec("apt-get update -qq && apt-get install -y -qq gcc").await?;
                if !inst.status.success() {
                    bail!("apt install gcc failed: {}", String::from_utf8_lossy(&inst.stderr));
                }
            } else {
                println!("  gcc already present");
            }

            let src = b"#include <stdio.h>
int main() { printf("hello-from-qlean-vm\n"); return 0; }
";
            println!("STEP 3: writing hello.c into the VM (via SFTP)...");
            vm.write("/tmp/h.c", src.as_slice()).await?;

            println!("STEP 4: compiling with gcc...");
            let cc = vm.exec("gcc /tmp/h.c -o /tmp/h").await?;
            if !cc.status.success() {
                bail!("gcc failed: {}", String::from_utf8_lossy(&cc.stderr));
            }

            println!("STEP 5: running compiled binary...");
            let run = vm.exec("/tmp/h").await?;
            println!("exit_code={}", run.status.code().unwrap_or(-1));
            println!("stdout={}", String::from_utf8_lossy(&run.stdout));
            println!("stderr={}", String::from_utf8_lossy(&run.stderr));

            if run.stdout.trim_ascii_end() == b"hello-from-qlean-vm" {
                println!("RESULT: PASS");
            } else {
                bail!("RESULT: FAIL, stdout mismatch");
            }
            Ok(())
        })
    }).await
}
