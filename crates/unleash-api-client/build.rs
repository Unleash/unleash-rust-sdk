use std::process::Command;

fn main() {
    let rustc = std::env::var_os("RUSTC").expect("Cargo did not provide RUSTC");

    let output = Command::new(rustc)
        .arg("--version")
        .output()
        .expect("failed to run rustc --version");

    assert!(
        output.status.success(),
        "rustc --version failed with status {}",
        output.status
    );

    let version = String::from_utf8(output.stdout).expect("rustc version was not valid UTF-8");

    println!("cargo:rustc-env=RUSTC_VERSION={}", version.trim());
}
