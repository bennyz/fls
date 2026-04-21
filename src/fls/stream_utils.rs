use crate::fls::byte_channel::ByteBoundedReceiver;
use bytes::Bytes;
use std::io::Read;
use tokio::sync::mpsc;

pub struct ChannelReader {
    rx: ByteBoundedReceiver<Bytes>,
    current: Option<Bytes>,
    offset: usize,
    progress_tx: Option<mpsc::UnboundedSender<u64>>,
}

impl ChannelReader {
    pub fn new_byte_bounded(rx: ByteBoundedReceiver<Bytes>) -> Self {
        Self {
            rx,
            current: None,
            offset: 0,
            progress_tx: None,
        }
    }

    pub fn with_progress(mut self, tx: mpsc::UnboundedSender<u64>) -> Self {
        self.progress_tx = Some(tx);
        self
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if let Some(ref data) = self.current {
                let remaining = &data[self.offset..];
                if !remaining.is_empty() {
                    let to_copy = remaining.len().min(buf.len());
                    buf[..to_copy].copy_from_slice(&remaining[..to_copy]);
                    self.offset += to_copy;
                    return Ok(to_copy);
                }
            }

            match self.rx.blocking_recv() {
                Some(data) => {
                    if let Some(ref tx) = self.progress_tx {
                        let _ = tx.send(data.len() as u64);
                    }
                    self.current = Some(data);
                    self.offset = 0;
                }
                None => {
                    return Ok(0);
                }
            }
        }
    }
}
