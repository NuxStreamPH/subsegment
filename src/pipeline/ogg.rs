//! Page framing and the two Opus header packets required by late listeners.
//! Only used for our own Opus/Ogg output, never arbitrary passthrough bytes.
use std::sync::Arc;

#[derive(Default)]
pub(super) struct OggFramer {
    pending: Vec<u8>,
    pub headers: Vec<u8>,
    packets: usize,
}

impl OggFramer {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Arc<Vec<u8>>> {
        self.pending.extend_from_slice(bytes);
        let mut pages = Vec::new();
        loop {
            if self.pending.len() < 27 {
                break;
            }
            if !self.pending.starts_with(b"OggS") {
                // Output from either backend must be Ogg; refuse unbounded
                // buffering if a broken encoder violates that contract.
                self.pending.clear();
                break;
            }
            let segments = self.pending[26] as usize;
            let head_len = 27 + segments;
            if self.pending.len() < head_len {
                break;
            }
            let page_len = head_len
                + self.pending[27..head_len]
                    .iter()
                    .map(|v| *v as usize)
                    .sum::<usize>();
            if self.pending.len() < page_len {
                break;
            }
            let page: Vec<u8> = self.pending.drain(..page_len).collect();
            if page[5] & 2 != 0 {
                self.headers.clear();
                self.packets = 0;
            }
            if self.packets < 2 {
                // Locally generated OpusHead/OpusTags are tiny. Bound the cache
                // even if a plugin unexpectedly emits excessive metadata.
                if self.headers.len() + page.len() <= 256 * 1024 {
                    self.headers.extend_from_slice(&page);
                }
                self.packets += page[27..head_len].iter().filter(|v| **v < 255).count();
            }
            pages.push(Arc::new(page));
        }
        pages
    }
}
