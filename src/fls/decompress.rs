use crate::fls::byte_channel::ByteBoundedReceiver;
use crate::fls::compression::Compression;
use crate::fls::magic_bytes::detect_compression;
use crate::fls::stream_utils::ChannelReader;
use bytes::Bytes;
use std::io::Read;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

pub(crate) fn mb_to_bytes(mb: u64) -> u64 {
    mb.saturating_mul(1024 * 1024)
}

pub(crate) fn create_xz_decoder<R: Read>(
    reader: R,
    memlimit_mb: u64,
) -> Result<liblzma::read::XzDecoder<R>, String> {
    let memlimit = mb_to_bytes(memlimit_mb);
    let stream = liblzma::stream::Stream::new_stream_decoder(memlimit, 0).map_err(|e| {
        format!(
            "Failed to create XZ decoder with {}MB limit: {}",
            memlimit_mb, e
        )
    })?;
    Ok(liblzma::read::XzDecoder::new_stream(reader, stream))
}

pub(crate) fn create_mt_xz_decoder<R: Read + Send + 'static>(
    reader: R,
    xz_memlimit_mb: u64,
) -> Result<Box<dyn Read + Send>, String> {
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(2);
    let memlimit = mb_to_bytes(xz_memlimit_mb);
    eprintln!(
        "XZ decompression: {} threads, memory limit {}MB",
        num_threads, xz_memlimit_mb
    );
    let stream = liblzma::stream::MtStreamBuilder::new()
        .threads(num_threads)
        .memlimit_threading(memlimit)
        .memlimit_stop(memlimit)
        .decoder()
        .map_err(|e| format!("Failed to create MT XZ decoder: {}", e))?;
    Ok(Box::new(liblzma::read::XzDecoder::new_stream(
        reader, stream,
    )))
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

pub(crate) fn get_compression_from_url(url: &str) -> Compression {
    let path = url.split('?').next().unwrap_or(url);
    let path = path.split('#').next().unwrap_or(path);
    let extension = path.rsplit('.').next().unwrap_or("").to_lowercase();
    match extension.as_str() {
        "gz" => Compression::Gzip,
        "xz" => Compression::Xz,
        "zst" | "zstd" => Compression::Zstd,
        _ => Compression::None,
    }
}

type DecompressorResult = (
    mpsc::Receiver<Vec<u8>>,
    std::thread::JoinHandle<Result<(), String>>,
);

pub(crate) fn start_inprocess_decompressor(
    buffer_rx: ByteBoundedReceiver<Bytes>,
    compression: Compression,
    consumed_progress_tx: mpsc::UnboundedSender<u64>,
    xz_memlimit_mb: u64,
) -> Result<DecompressorResult, Box<dyn std::error::Error>> {
    let reader = ChannelReader::new_byte_bounded(buffer_rx).with_progress(consumed_progress_tx);
    spawn_decoder_thread(reader, Some(compression), xz_memlimit_mb)
}

/// Like `start_inprocess_decompressor`, but the compression format is detected
/// from the stream's magic bytes instead of being known up front.
pub(crate) fn start_inprocess_decompressor_autodetect(
    reader: ChannelReader,
    xz_memlimit_mb: u64,
) -> Result<DecompressorResult, Box<dyn std::error::Error>> {
    spawn_decoder_thread(reader, None, xz_memlimit_mb)
}

fn read_prefix(reader: &mut dyn Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

fn spawn_decoder_thread(
    reader: ChannelReader,
    compression: Option<Compression>,
    xz_memlimit_mb: u64,
) -> Result<DecompressorResult, Box<dyn std::error::Error>> {
    let (decompressed_tx, decompressed_rx) = mpsc::channel::<Vec<u8>>(8);

    let handle = std::thread::Builder::new()
        .name("decompressor".to_string())
        .spawn(move || {
            let mut input: Box<dyn Read + Send> = Box::new(reader);

            let compression = match compression {
                Some(c) => c,
                None => {
                    let mut prefix = [0u8; 6];
                    let n = read_prefix(&mut input, &mut prefix)
                        .map_err(|e| format!("Failed to read stream header: {}", e))?;
                    let detected = detect_compression(&prefix[..n]);
                    input = Box::new(std::io::Cursor::new(prefix[..n].to_vec()).chain(input));
                    detected
                }
            };

            let mut decoder: Box<dyn Read + Send> = match compression {
                Compression::Xz => create_mt_xz_decoder(input, xz_memlimit_mb)?,
                Compression::Gzip => Box::new(flate2::read::GzDecoder::new(input)),
                Compression::None => input,
                Compression::Zstd => {
                    return Err("Zstd in-process decompression is not supported".to_string());
                }
            };

            let mut buf = vec![0u8; 8 * 1024 * 1024];
            loop {
                let n = decoder
                    .read(&mut buf)
                    .map_err(|e| format!("Decompression error: {}", e))?;
                if n == 0 {
                    break;
                }
                if decompressed_tx.blocking_send(buf[..n].to_vec()).is_err() {
                    return Err("Writer task closed, stopping decompression".to_string());
                }
            }
            Ok(())
        })
        .map_err(|e| -> Box<dyn std::error::Error> {
            format!("Failed to spawn decompressor thread: {}", e).into()
        })?;

    Ok((decompressed_rx, handle))
}
