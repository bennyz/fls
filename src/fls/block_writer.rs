use std::alloc::{alloc, dealloc, Layout};
use std::fs::OpenOptions;
use std::io::{self, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::ptr::NonNull;
use tokio::sync::mpsc;

const BLOCK_SIZE: usize = 1024 * 1024; // 1MB blocks for better throughput
const ALIGNMENT: usize = 4096; // 4KB alignment for direct I/O

/// Smallest zero fill worth handing to the kernel. Below this the fill goes
/// through the write buffer like any other data, so runs of small FILL chunks
/// interleaved with RAW data do not break the stream of 1 MiB writes.
const ZERO_FAST_PATH_MIN_BYTES: u64 = 1024 * 1024;

#[cfg(target_os = "macos")]
#[allow(dead_code)]
const F_NOCACHE: i32 = 48; // F_NOCACHE for macOS (use with fcntl)

// Linux ioctl definitions using nix crate's ioctl macros for safety
#[cfg(target_os = "linux")]
mod linux_ioctl {
    use nix::{ioctl_read, ioctl_write_ptr_bad, request_code_none};
    // BLKGETSIZE64 - get device size in bytes (u64)
    // This uses the nix crate to generate the correct ioctl number for the target architecture
    ioctl_read!(blkgetsize64, 0x12, 114, u64);
    // BLKZEROOUT - zero the byte range [offset, len] directly on the device
    ioctl_write_ptr_bad!(blkzeroout, request_code_none!(0x12, 127), [u64; 2]);
}

#[cfg(target_os = "macos")]
#[allow(dead_code)]
const DKIOCGETBLOCKCOUNT: libc::c_ulong = 0x40086419;

#[cfg(target_os = "macos")]
#[allow(dead_code)]
const DKIOCGETBLOCKSIZE: libc::c_ulong = 0x40046418;

/// Aligned buffer for direct I/O operations
/// This ensures the buffer memory is aligned to ALIGNMENT bytes as required by O_DIRECT
struct AlignedBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl AlignedBuffer {
    fn new(size: usize, alignment: usize) -> Self {
        let layout = Layout::from_size_align(size, alignment).expect("Invalid layout");
        let ptr = unsafe {
            let raw_ptr = alloc(layout);
            if raw_ptr.is_null() {
                panic!("Failed to allocate aligned buffer");
            }
            // Zero the buffer
            std::ptr::write_bytes(raw_ptr, 0, size);
            NonNull::new_unchecked(raw_ptr)
        };
        Self { ptr, layout }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

// Safety: AlignedBuffer owns its data and the pointer is valid for the lifetime of the struct
unsafe impl Send for AlignedBuffer {}

/// Opens a file for a device (block or character) - read+write, no create/truncate
fn open_for_device(path: &str) -> io::Result<std::fs::File> {
    OpenOptions::new().read(true).write(true).open(path)
}

/// Opens a file for a regular file - write, create, truncate
fn open_for_regular_file(path: &str) -> io::Result<std::fs::File> {
    if path.starts_with("/dev/") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Regular file path cannot be under /dev/; ensure the device path is correctly spelled",
        ));
    }
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
}

/// Opens a file for a device with custom flags
fn open_for_device_with_flags(path: &str, flags: libc::c_int) -> io::Result<std::fs::File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(flags)
        .open(path)
}

/// Opens a file for a regular file with custom flags
fn open_for_regular_file_with_flags(path: &str, flags: libc::c_int) -> io::Result<std::fs::File> {
    if path.starts_with("/dev/") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Regular file path cannot be under /dev/; ensure the device path is correctly spelled",
        ));
    }
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(flags)
        .open(path)
}

/// BlockWriter handles writing data to a block device with direct I/O
pub(crate) struct BlockWriter {
    file: std::fs::File,
    buffer: AlignedBuffer,
    buffer_pos: usize, // Current position in buffer
    bytes_written: u64,
    bytes_since_sync: u64, // Track bytes written since last sync
    written_progress_tx: mpsc::UnboundedSender<u64>,
    use_direct_io: bool, // Track if O_DIRECT is active
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    is_block_device: bool,
    /// Whether large zero fills are handed to the kernel (BLKZEROOUT or
    /// fallocate). Off for block devices without native WRITE ZEROES, where
    /// the kernel would just emulate it with ordinary writes, and cleared for
    /// good once the fast path fails.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    zero_fast_path: bool,
    #[allow(dead_code)]
    debug: bool, // Debug mode flag
}

// Sync every 64MB when not using direct I/O (Linux only - macOS doesn't sync during writes)
#[cfg(not(target_os = "macos"))]
const SYNC_INTERVAL: u64 = 64 * 1024 * 1024;

impl BlockWriter {
    /// Open a block device for writing with direct I/O
    pub(crate) fn new(
        device: &str,
        written_progress_tx: mpsc::UnboundedSender<u64>,
        debug: bool,
        o_direct: bool,
    ) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        let (file, use_direct_io, is_block_device) = {
            use std::os::unix::fs::FileTypeExt;

            // Check if this is a block device
            // Handle non-existent files (they'll be created as regular files)
            let is_block_dev = match std::fs::metadata(device) {
                Ok(m) => m.file_type().is_block_device(),
                Err(e) if e.kind() == io::ErrorKind::NotFound => false,
                Err(e) => return Err(e),
            };

            // Helper to open file with appropriate method based on device type
            let open_file = |flags: Option<libc::c_int>| -> io::Result<std::fs::File> {
                match flags {
                    Some(f) if is_block_dev => open_for_device_with_flags(device, f),
                    Some(f) => open_for_regular_file_with_flags(device, f),
                    None if is_block_dev => open_for_device(device),
                    None => open_for_regular_file(device),
                }
            };

            if o_direct {
                // Try O_DIRECT without O_SYNC first (some devices have issues with both)
                match open_file(Some(libc::O_DIRECT)) {
                    Ok(f) => {
                        if debug {
                            eprintln!(
                                "[DEBUG] Opened {} with O_DIRECT (direct I/O enabled)",
                                device
                            );
                        }
                        (f, true, is_block_dev)
                    }
                    Err(e1) => {
                        // Try O_DIRECT with O_SYNC
                        match open_file(Some(libc::O_DIRECT | libc::O_SYNC)) {
                            Ok(f) => {
                                if debug {
                                    eprintln!(
                                        "[DEBUG] Opened {} with O_DIRECT|O_SYNC (direct I/O enabled)",
                                        device
                                    );
                                }
                                (f, true, is_block_dev)
                            }
                            Err(e2) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::Unsupported,
                                    format!(
                                        "O_DIRECT not supported on {} (tried without O_SYNC: {}, with O_SYNC: {})",
                                        device, e1, e2
                                    ),
                                ));
                            }
                        }
                    }
                }
            } else {
                if debug {
                    eprintln!(
                        "[DEBUG] Opened {} with buffered I/O (O_DIRECT not requested)",
                        device
                    );
                }
                (open_file(None)?, false, is_block_dev)
            }
        };

        #[cfg(target_os = "macos")]
        let (file, use_direct_io, is_block_device) = {
            use std::os::unix::fs::FileTypeExt;
            use std::os::unix::io::AsRawFd;

            // Check if this is a block device or character device (raw disk)
            // Handle non-existent files (they'll be created as regular files)
            let (is_block_dev, is_char_dev, is_device, file_exists) =
                match std::fs::metadata(device) {
                    Ok(metadata) => {
                        let file_type = metadata.file_type();
                        let is_block = file_type.is_block_device();
                        let is_char = file_type.is_char_device();
                        (is_block, is_char, is_block || is_char, true)
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => (false, false, false, false),
                    Err(e) => return Err(e),
                };

            if debug {
                eprintln!("[DEBUG] Device type check for {}:", device);
                eprintln!("[DEBUG]   File exists: {}", file_exists);
                eprintln!("[DEBUG]   Block device: {}", is_block_dev);
                eprintln!("[DEBUG]   Character device (raw): {}", is_char_dev);
                eprintln!("[DEBUG]   Regular file: {}", file_exists && !is_device);
            }

            // On macOS, strongly warn if using /dev/diskN instead of /dev/rdiskN
            // Buffered block devices often fail with ioctl errors when writing raw data
            if is_block_dev && device.starts_with("/dev/disk") && !device.starts_with("/dev/rdisk")
            {
                eprintln!("\nWARNING: You are using buffered device {}", device);
                eprintln!("   On macOS, buffered block devices (/dev/diskN) often fail with 'Inappropriate ioctl' errors");
                eprintln!(
                    "   when writing raw disk images, especially if the disk has partitions."
                );
                eprintln!("   \n   STRONGLY RECOMMENDED: Use the raw device instead:");
                eprintln!("   {}", device.replace("/dev/disk", "/dev/rdisk"));
                eprintln!();
            }

            // Helper to open file with appropriate method based on device type
            let open_file = |flags: Option<libc::c_int>| -> io::Result<std::fs::File> {
                match flags {
                    Some(f) if is_device => open_for_device_with_flags(device, f),
                    Some(f) => open_for_regular_file_with_flags(device, f),
                    None if is_device => open_for_device(device),
                    None => open_for_regular_file(device),
                }
            };

            if o_direct {
                // macOS uses F_NOCACHE instead of O_DIRECT
                let f = open_file(Some(libc::O_SYNC))?;
                let fd = f.as_raw_fd();

                // Enable F_NOCACHE to bypass filesystem cache
                if unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) } == -1 {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "F_NOCACHE not supported on this device",
                    ));
                }
                if debug {
                    eprintln!(
                        "[DEBUG] Opened {} with F_NOCACHE (direct I/O enabled)",
                        device
                    );
                }
                (f, true, is_block_dev)
            } else {
                if debug {
                    eprintln!(
                        "[DEBUG] Opened {} with buffered I/O (O_DIRECT not requested)",
                        device
                    );
                }
                (open_file(None)?, false, is_block_dev)
            }
        };

        #[cfg(target_os = "linux")]
        let zero_fast_path = if is_block_device {
            let native = supports_write_zeroes(&file);
            if debug && !native {
                eprintln!(
                    "[DEBUG] {} has no native WRITE ZEROES support, zero fills are written normally",
                    device
                );
            }
            native
        } else {
            true
        };
        #[cfg(not(target_os = "linux"))]
        let zero_fast_path = false;

        // Create aligned buffer for direct I/O
        // Direct I/O requires buffer to be aligned to sector size (typically 512 or 4096)
        let buffer = AlignedBuffer::new(BLOCK_SIZE, ALIGNMENT);

        Ok(Self {
            file,
            buffer,
            buffer_pos: 0,
            bytes_written: 0,
            bytes_since_sync: 0,
            written_progress_tx,
            use_direct_io,
            is_block_device,
            zero_fast_path,
            debug,
        })
    }

    /// Write data to the block device
    /// Data is buffered internally and written in larger blocks for better performance
    pub(crate) fn write(&mut self, data: &[u8]) -> io::Result<()> {
        let mut offset = 0;

        while offset < data.len() {
            let remaining_in_buffer = BLOCK_SIZE - self.buffer_pos;
            let remaining_in_data = data.len() - offset;
            let to_copy = remaining_in_buffer.min(remaining_in_data);

            // Copy to internal buffer
            let buffer_slice = self.buffer.as_mut_slice();
            buffer_slice[self.buffer_pos..self.buffer_pos + to_copy]
                .copy_from_slice(&data[offset..offset + to_copy]);

            self.buffer_pos += to_copy;
            offset += to_copy;
            self.bytes_written += to_copy as u64;

            // Flush buffer when full
            if self.buffer_pos == BLOCK_SIZE {
                self.flush_buffer()?;
            }

            // Send progress update periodically (every 256KB to reduce overhead)
            if self.bytes_written.is_multiple_of(256 * 1024) {
                let _ = self.written_progress_tx.send(self.bytes_written);
            }
        }

        Ok(())
    }

    /// Write `bytes` bytes of a repeating 4-byte pattern at the current position.
    ///
    /// Large all-zero fills are offloaded to the kernel where that avoids the
    /// transfer entirely: BLKZEROOUT on block devices with native WRITE ZEROES,
    /// fallocate on regular files. Everything else goes through the regular
    /// buffered write path.
    pub(crate) fn fill(&mut self, pattern: &[u8; 4], bytes: u64) -> io::Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        if *pattern == [0u8; 4]
            && bytes >= ZERO_FAST_PATH_MIN_BYTES
            && self.zero_range_fast(bytes)?
        {
            return Ok(());
        }

        const FILL_BUFFER_SIZE: usize = 4096;
        let mut buffer = [0u8; FILL_BUFFER_SIZE];
        for chunk in buffer.as_chunks_mut::<4>().0 {
            *chunk = *pattern;
        }

        let mut remaining = bytes;
        while remaining > 0 {
            let to_write = (remaining as usize).min(FILL_BUFFER_SIZE);
            self.write(&buffer[..to_write])?;
            remaining -= to_write as u64;
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn zero_range_fast(&mut self, _bytes: u64) -> io::Result<bool> {
        Ok(false)
    }

    /// Zero `bytes` bytes at the current position without writing them from
    /// user space. Returns Ok(false) when the fast path is unavailable, in
    /// which case nothing has been written and the caller must fill normally.
    #[cfg(target_os = "linux")]
    fn zero_range_fast(&mut self, bytes: u64) -> io::Result<bool> {
        use std::os::unix::io::AsRawFd;

        if !self.zero_fast_path {
            return Ok(false);
        }

        let start = self.bytes_written;
        let end = start + bytes;
        // BLKZEROOUT needs sector alignment; O_DIRECT additionally needs the
        // following write to land on an aligned offset.
        let align = if self.use_direct_io {
            ALIGNMENT as u64
        } else {
            512
        };
        if !start.is_multiple_of(align) || !end.is_multiple_of(align) {
            return Ok(false);
        }

        self.flush_buffer()?;

        let result = if self.is_block_device {
            let range = [start, bytes];
            unsafe { linux_ioctl::blkzeroout(self.file.as_raw_fd(), &range) }
                .map(|_| ())
                .map_err(|e| io::Error::from_raw_os_error(e as i32))
        } else {
            fallocate_zero(&self.file, start, bytes)
        };

        match result {
            Ok(()) => {
                self.file.seek(io::SeekFrom::Start(end))?;
                self.bytes_written = end;
                let _ = self.written_progress_tx.send(self.bytes_written);
                Ok(true)
            }
            Err(e) if is_unsupported(&e) => {
                if self.debug {
                    eprintln!(
                        "[DEBUG] Kernel zero-fill unavailable ({}), writing zeros instead",
                        e
                    );
                }
                self.zero_fast_path = false;
                self.file.seek(io::SeekFrom::Start(start))?;
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Seek to an absolute position in the output
    ///
    /// This flushes any buffered data and seeks to the specified offset.
    /// Used by sparse image support to skip over DONT_CARE regions.
    ///
    /// For O_DIRECT mode, the offset must be aligned to ALIGNMENT (4096 bytes).
    pub(crate) fn seek(&mut self, offset: u64) -> io::Result<()> {
        // For O_DIRECT, validate offset alignment
        if self.use_direct_io && !offset.is_multiple_of(ALIGNMENT as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "O_DIRECT requires aligned seek offset: {} is not aligned to {}",
                    offset, ALIGNMENT
                ),
            ));
        }

        // Flush any pending data first
        self.flush_buffer()?;

        // Seek to the new position
        self.file.seek(io::SeekFrom::Start(offset))?;

        // Update our position tracking
        self.bytes_written = offset;

        // Send progress update
        let _ = self.written_progress_tx.send(self.bytes_written);

        Ok(())
    }

    /// Flush the internal buffer to disk
    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer_pos == 0 {
            return Ok(());
        }

        let actual_bytes = self.buffer_pos;
        let write_size = if self.use_direct_io && actual_bytes < BLOCK_SIZE {
            // For direct I/O, align to sector size
            let aligned = actual_bytes.div_ceil(ALIGNMENT) * ALIGNMENT;
            // Zero the padding region to avoid writing stale data
            let buffer_slice = self.buffer.as_mut_slice();
            for byte in buffer_slice[actual_bytes..aligned].iter_mut() {
                *byte = 0;
            }
            aligned
        } else {
            actual_bytes
        };

        let write_size = write_size.min(BLOCK_SIZE);

        let buffer_slice = self.buffer.as_slice();
        self.file
            .write_all(&buffer_slice[..write_size])
            .map_err(|e| {
                eprintln!("Write error at offset {}: {}", self.bytes_written, e);
                #[cfg(target_os = "macos")]
                {
                    // On macOS, error 25 (ENOTTY) often indicates disk is mounted or wrong device type
                    if e.raw_os_error() == Some(25) {
                        eprintln!("Error 25 (Inappropriate ioctl) on macOS - see detailed error message above.");
                    }
                }
                e
            })?;

        // Track actual bytes for sync interval, not padded write size
        self.bytes_since_sync += actual_bytes as u64;
        self.buffer_pos = 0;

        // For buffered I/O, sync periodically to avoid losing too much data on failure
        // On macOS, don't sync during writes to block devices (matches dd behavior)
        // Syncing during writes can cause "Inappropriate ioctl" errors on macOS
        #[cfg(target_os = "macos")]
        {
            // On macOS, don't sync during writes - only sync at the end
            // This matches dd's behavior and avoids ioctl errors
        }
        #[cfg(not(target_os = "macos"))]
        {
            if !self.use_direct_io && self.bytes_since_sync >= SYNC_INTERVAL {
                self.file.sync_data()?;
                self.bytes_since_sync = 0;
            }
        }

        Ok(())
    }

    /// Flush any remaining data and sync to disk
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        // On macOS, avoid flush() and sync() on block devices as they can cause ioctl errors
        // The data is already written to the kernel buffer, and macOS will flush it automatically
        #[cfg(target_os = "macos")]
        {
            // On macOS block devices, flush() and sync() can cause "Inappropriate ioctl" errors
            // Since we're writing directly to the device, the data is already in the kernel buffer
            // macOS will handle the final flush automatically when the file is closed
            // This matches dd's behavior - it doesn't explicitly sync on macOS block devices
        }
        #[cfg(not(target_os = "macos"))]
        {
            // On Linux, we can safely flush and sync
            self.file.flush()?;
            self.file.sync_all()?;
        }
        self.bytes_since_sync = 0;
        // Send final progress update
        let _ = self.written_progress_tx.send(self.bytes_written);
        Ok(())
    }

    /// Get total bytes written
    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Command sent to the async block writer
#[derive(Debug)]
pub(crate) enum WriterCommand {
    /// Write data at current position
    Write(Vec<u8>),
    /// Seek to absolute offset
    Seek(u64),
    /// Write fill pattern for specified number of bytes
    Fill { pattern: [u8; 4], bytes: u64 },
}

/// Async wrapper for BlockWriter that runs in a blocking task
pub(crate) struct AsyncBlockWriter {
    writer_tx: mpsc::Sender<WriterCommand>,
    writer_handle: tokio::task::JoinHandle<io::Result<u64>>,
}

impl AsyncBlockWriter {
    /// Create a new async block writer
    pub(crate) fn new(
        device: String,
        written_progress_tx: mpsc::UnboundedSender<u64>,
        debug: bool,
        o_direct: bool,
        write_buffer_size_mb: usize,
    ) -> io::Result<Self> {
        // Calculate channel capacity based on buffer size
        // Assuming 8MB chunks from decompressor reader (see from_url.rs line 47)
        const CHUNK_SIZE_MB: usize = 8;
        let channel_capacity = (write_buffer_size_mb / CHUNK_SIZE_MB).max(1);

        if debug {
            eprintln!(
                "[DEBUG] Write buffer: {} MB (capacity: {} chunks of {}MB each)",
                write_buffer_size_mb, channel_capacity, CHUNK_SIZE_MB
            );
        }

        // Create bounded channel to prevent OOM when decompression outpaces disk writes
        // This provides backpressure when writing is slower than decompression
        let (writer_tx, mut writer_rx) = mpsc::channel::<WriterCommand>(channel_capacity);

        // Spawn blocking task for I/O operations
        let writer_handle = tokio::task::spawn_blocking(move || {
            let mut writer = BlockWriter::new(&device, written_progress_tx, debug, o_direct)
                .map_err(|e| {
                    eprintln!("Failed to open device '{}': {}", device, e);
                    e
                })?;

            while let Some(cmd) = writer_rx.blocking_recv() {
                let result = match cmd {
                    WriterCommand::Write(data) => writer.write(&data),
                    WriterCommand::Seek(offset) => writer.seek(offset),
                    WriterCommand::Fill { pattern, bytes } => writer.fill(&pattern, bytes),
                };

                if let Err(e) = result {
                    eprintln!("Failed I/O operation on device '{}': {}", device, e);
                    #[cfg(target_os = "macos")]
                    {
                        // On macOS, provide helpful hints for common errors
                        if e.raw_os_error() == Some(25) {
                            eprintln!("\n⚠️  Error 25 (Inappropriate ioctl) on macOS:");
                            if device.starts_with("/dev/disk") && !device.starts_with("/dev/rdisk")
                            {
                                eprintln!(
                                    "  You MUST use the raw device: {}",
                                    device.replace("/dev/disk", "/dev/rdisk")
                                );
                                eprintln!("  Buffered devices (/dev/diskN) cannot be used for raw disk writes.");
                            } else {
                                eprintln!(
                                    "  Try unmounting the disk: diskutil unmountDisk {}",
                                    device
                                );
                                eprintln!("  Or ejecting it: diskutil eject {}", device);
                            }
                        }
                    }
                    return Err(e);
                }
            }

            writer.flush()?;
            Ok(writer.bytes_written())
        });

        Ok(Self {
            writer_tx,
            writer_handle,
        })
    }

    /// Write data asynchronously
    pub(crate) async fn write(&self, data: Vec<u8>) -> io::Result<()> {
        self.writer_tx
            .send(WriterCommand::Write(data))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Writer channel closed"))?;
        Ok(())
    }

    /// Seek to absolute offset asynchronously
    pub(crate) async fn seek(&self, offset: u64) -> io::Result<()> {
        self.writer_tx
            .send(WriterCommand::Seek(offset))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Writer channel closed"))?;
        Ok(())
    }

    /// Write fill pattern asynchronously
    pub(crate) async fn fill(&self, pattern: [u8; 4], bytes: u64) -> io::Result<()> {
        self.writer_tx
            .send(WriterCommand::Fill { pattern, bytes })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Writer channel closed"))?;
        Ok(())
    }

    /// Close the writer and wait for completion
    pub(crate) async fn close(self) -> io::Result<u64> {
        drop(self.writer_tx);
        self.writer_handle.await.map_err(io::Error::other)?
    }
}

/// Whether a block device can zero a range without transferring data
/// (`write_zeroes_max_bytes` in sysfs is non-zero). Without that the kernel
/// emulates BLKZEROOUT with ordinary writes, which gains nothing over the
/// write buffer and costs a stream split per fill. Unknown means no.
#[cfg(target_os = "linux")]
fn supports_write_zeroes(file: &std::fs::File) -> bool {
    use std::os::unix::fs::MetadataExt;

    let Ok(meta) = file.metadata() else {
        return false;
    };
    let rdev = meta.rdev();
    let (major, minor) = (libc::major(rdev), libc::minor(rdev));
    // Whole disks have queue/ directly; partitions find it on the parent disk.
    let candidates = [
        format!(
            "/sys/dev/block/{}:{}/queue/write_zeroes_max_bytes",
            major, minor
        ),
        format!(
            "/sys/dev/block/{}:{}/../queue/write_zeroes_max_bytes",
            major, minor
        ),
    ];
    candidates
        .iter()
        .find_map(|path| std::fs::read_to_string(path).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .is_some_and(|max_bytes| max_bytes > 0)
}

/// Errors that mean the kernel zero-fill path is not available for this
/// file or device (as opposed to a real I/O failure).
#[cfg(target_os = "linux")]
fn is_unsupported(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ENOTTY)
            | Some(libc::EOPNOTSUPP)
            | Some(libc::ENOSYS)
            | Some(libc::EINVAL)
            | Some(libc::EPERM)
            | Some(libc::ENODEV)
    )
}

/// Zero a byte range of a regular file through fallocate.
///
/// FALLOC_FL_ZERO_RANGE is preferred; filesystems without it (e.g. tmpfs)
/// get a hole punched instead, and the file is extended so the range reads
/// back as zeros either way.
#[cfg(target_os = "linux")]
fn fallocate_zero(file: &std::fs::File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    let to_off = |v: u64| {
        libc::off_t::try_from(v)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "zero range exceeds off_t"))
    };
    let (off, ln) = (to_off(offset)?, to_off(len)?);

    if unsafe { libc::fallocate(fd, libc::FALLOC_FL_ZERO_RANGE, off, ln) } == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if !is_unsupported(&err) {
        return Err(err);
    }

    let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
    if unsafe { libc::fallocate(fd, mode, off, ln) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let end = offset + len;
    if file.metadata()?.len() < end {
        file.set_len(end)?;
    }
    Ok(())
}

/// Check if a path is a block device
#[allow(dead_code)]
pub(crate) fn is_block_device(path: &str) -> io::Result<bool> {
    use std::os::unix::fs::FileTypeExt;
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.file_type().is_block_device())
}

/// Get block device size in bytes
#[allow(dead_code)]
pub(crate) fn get_device_size(path: &str) -> io::Result<u64> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_RDONLY)
        .open(path)?;

    let fd = file.as_raw_fd();

    #[cfg(target_os = "linux")]
    {
        let mut size: u64 = 0;
        // Use nix crate's generated ioctl - this is architecture-safe
        unsafe {
            linux_ioctl::blkgetsize64(fd, &mut size)
                .map_err(|e| io::Error::from_raw_os_error(e as i32))?;
        }
        Ok(size)
    }

    #[cfg(target_os = "macos")]
    {
        unsafe {
            let mut size: u64 = 0;
            if libc::ioctl(fd, DKIOCGETBLOCKCOUNT, &mut size) == 0 {
                let mut block_size: u32 = 0;
                if libc::ioctl(fd, DKIOCGETBLOCKSIZE, &mut block_size) == 0 {
                    Ok(size * block_size as u64)
                } else {
                    Err(io::Error::last_os_error())
                }
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer_for(path: &std::path::Path) -> (BlockWriter, mpsc::UnboundedReceiver<u64>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let writer = BlockWriter::new(path.to_str().unwrap(), tx, false, false).unwrap();
        (writer, rx)
    }

    #[test]
    fn zero_fill_regular_file_reads_back_as_zeros_and_extends_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.img");
        let (mut writer, _rx) = writer_for(&path);

        writer.write(&[0xAA; 8192]).unwrap();
        writer.fill(&[0, 0, 0, 0], 4096).unwrap(); // below the fast-path threshold
        writer.fill(&[0, 0, 0, 0], 1024 * 1024).unwrap();
        writer.write(&[0xBB; 4096]).unwrap();
        writer.fill(&[0, 0, 0, 0], 2 * 1024 * 1024).unwrap();
        writer.flush().unwrap();

        let zeros1 = 4096 + 1024 * 1024;
        let expected_len = 8192 + zeros1 + 4096 + 2 * 1024 * 1024;
        assert_eq!(writer.bytes_written(), expected_len as u64);
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), expected_len);
        assert!(data[..8192].iter().all(|&b| b == 0xAA));
        assert!(data[8192..8192 + zeros1].iter().all(|&b| b == 0));
        let raw2 = 8192 + zeros1;
        assert!(data[raw2..raw2 + 4096].iter().all(|&b| b == 0xBB));
        assert!(data[raw2 + 4096..].iter().all(|&b| b == 0));
    }

    #[test]
    fn unaligned_zero_fill_falls_back_to_plain_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.img");
        let (mut writer, _rx) = writer_for(&path);

        writer.write(&[0x11; 100]).unwrap();
        writer.fill(&[0, 0, 0, 0], 1000).unwrap();
        writer.write(&[0x22; 50]).unwrap();
        writer.flush().unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 1150);
        assert!(data[..100].iter().all(|&b| b == 0x11));
        assert!(data[100..1100].iter().all(|&b| b == 0));
        assert!(data[1100..].iter().all(|&b| b == 0x22));
    }

    #[test]
    fn pattern_fill_repeats_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.img");
        let (mut writer, _rx) = writer_for(&path);

        writer.fill(&[0xDE, 0xAD, 0xBE, 0xEF], 10000).unwrap();
        writer.flush().unwrap();

        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), 10000);
        for (i, &b) in data.iter().enumerate() {
            assert_eq!(b, [0xDE, 0xAD, 0xBE, 0xEF][i % 4]);
        }
    }
}
