// qlean-project-demo: tar pack -> unpack -> qlean upload -> make -> run.
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
    // 1. pack project dir into tar
    let mut tar = tar::Builder::new(Vec::new());
    tar.append_dir_all(".", "./testproj")?;
    let tar_bytes = tar.into_inner()?;

    // 2. unpack into a temp dir as the executor would
    let tmp = tempfile::tempdir()?;
    let unpacked = tmp.path().join("testproj");
    std::fs::create_dir_all(&unpacked)?;
    let mut archive = tar::Archive::new(tar_bytes.as_slice());
    archive.unpack(&unpacked)?;
    let upload_src = unpacked.to_string_lossy().into_owned();

    // 3. run in qlean VM
    let image = Image::new(ImageConfig::default()).await?;
    let config = MachineConfig::default();

    with_machine(&image, &config, |vm| {
        Box::pin(async move {
            // qlean mirrors dir into remote_path/basename; upload to parent
            vm.upload(upload_src.as_str(), "/root").await?;

            // default cloud image has no toolchain; install gcc first
            let _ = vm.exec("apt-get update -qq && apt-get install -y -qq gcc").await?;
            let build = vm.exec("cd /root/testproj && gcc main.c -o app").await?;
            if !build.status.success() {
                bail!("gcc failed: {}", String::from_utf8_lossy(&build.stderr));
            }

            let run = vm.exec("cd /root/testproj && ./app").await?;
            let stdout = String::from_utf8_lossy(&run.stdout);
            println!("stdout={}", stdout);
            if run.stdout.trim_ascii_end() == b"project-hello" {
                println!("RESULT: PASS");
            } else {
                bail!("stdout mismatch")
            }
            Ok(())
        })
    }).await
}
