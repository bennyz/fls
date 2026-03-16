use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

/// Determines the appropriate decompression command based on URL extension
fn get_decompressor_command(url: &str) -> &'static str {
    let extension = url.rsplit('.').next().unwrap_or("").to_lowercase();
    match extension.as_str() {
        "gz" => "zcat",
        "xz" => "xzcat",
        "bz" | "bz2" => "bzcat",
        _ => "cat", // Unknown extension, assume uncompressed
    }
}

/// Checks if a binary is available on the system
pub(crate) fn check_binary_available(cmd: &str) -> Result<(), String> {
    match std::process::Command::new(cmd)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "Required binary '{}' not found in PATH. Please install it and try again.",
            cmd
        )),
    }
}

/// Starts the appropriate decompression process based on URL extension
pub(crate) async fn start_decompressor_process(
    url: &str,
) -> Result<(Child, &'static str), Box<dyn std::error::Error>> {
    let cmd = get_decompressor_command(url);

    check_binary_available(cmd)?;
    eprintln!("Using decompressor: {}", cmd);
    spawn_decompressor(cmd)
}

/// Maps a Compression enum to the corresponding decompressor command
pub(crate) fn decompressor_for_compression(
    compression: crate::fls::compression::Compression,
) -> &'static str {
    use crate::fls::compression::Compression;
    match compression {
        Compression::Gzip => "zcat",
        Compression::Xz => "xzcat",
        Compression::Zstd => "zstdcat",
        Compression::None => "cat",
    }
}

/// Starts a decompressor process based on detected compression type
pub(crate) fn start_decompressor_for_compression(
    compression: crate::fls::compression::Compression,
) -> Result<(Child, &'static str), Box<dyn std::error::Error>> {
    let cmd = decompressor_for_compression(compression);
    check_binary_available(cmd)?;
    eprintln!("Using decompressor: {}", cmd);
    spawn_decompressor(cmd)
}

/// Spawns a decompressor subprocess with piped stdin/stdout/stderr
fn spawn_decompressor(
    cmd: &'static str,
) -> Result<(Child, &'static str), Box<dyn std::error::Error>> {
    let process = Command::new(cmd)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    Ok((process, cmd))
}

pub(crate) async fn spawn_stderr_reader(
    mut stderr: tokio::process::ChildStderr,
    error_tx: mpsc::UnboundedSender<String>,
    process_name: &'static str,
) {
    let mut buffer = [0u8; 1024];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                if let Ok(s) = String::from_utf8(buffer[..n].to_vec()) {
                    let _ = error_tx.send(format!("{}: {}", process_name, s.trim()));
                }
            }
            Err(_) => break,
        }
    }
}
