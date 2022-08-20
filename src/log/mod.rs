mod config;
mod index;
mod segment;
pub mod store;

#[cfg(test)]
mod tests;

use std::{
    fs::{read_dir, remove_file},
    io::Read,
    mem,
    path::Path,
    sync::RwLock,
    vec,
};

use crate::server::record::PRecord;

use anyhow::anyhow;

use {config::Config, segment::Segment, store::Store};

struct Log {
    dir: String,
    config: Config,
    inner_segments: RwLock<InnerSegments>,
}

struct InnerSegments {
    active_segment_index: usize,
    segments: Vec<Segment>,
}

struct OriginReader<'a> {
    store: &'a mut Store,
    off: u64,
}

impl Read for OriginReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.store.read_at(&mut buf.to_vec(), self.off)?;
        self.off += TryInto::<u64>::try_into(n).unwrap();
        Ok(n)
    }
}

impl Log {
    fn new_log(dir: String, mut config: Config) -> anyhow::Result<Self> {
        if config.max_store_bytes == 0 {
            config.max_store_bytes = 1024;
        }
        if config.max_index_bytes == 0 {
            config.max_index_bytes = 1024;
        }

        let log = Log {
            dir: dir.clone(),
            config: config.clone(),
            inner_segments: RwLock::new(InnerSegments {
                active_segment_index: 0,
                segments: vec![],
            }),
        };

        {
            let mut inner_segments = log.inner_segments.write().unwrap();

            let mut base_offsets = vec![];
            for entry in read_dir(&dir)? {
                let entry = entry?;
                if let Some(file_stem) = entry.path().file_stem() {
                    let file_stem = file_stem.to_str().unwrap();
                    let off: u64 = file_stem.parse().unwrap();
                    base_offsets.push(off);
                }
            }
            base_offsets.sort();
            let mut count = 0;
            for off in base_offsets.iter().step_by(2) {
                Log::new_segment(&dir, config.clone(), &mut inner_segments, *off)?;
                count += 1;
            }
            if count == 0 {
                Log::new_segment(&dir, config, &mut inner_segments, log.config.initial_offset)?;
            }
        }
        Ok(log)
    }

    fn append(&mut self, record: &mut PRecord) -> anyhow::Result<u64> {
        let mut inner_segments = self.inner_segments.write().unwrap();

        let highest_offset = Log::highest_offset_inner(&inner_segments.segments);

        let index = inner_segments.active_segment_index;
        if inner_segments.segments[index].is_maxed()? {
            Log::new_segment(
                &self.dir,
                self.config.clone(),
                &mut inner_segments,
                highest_offset + 1,
            )?;
        }

        let index = inner_segments.active_segment_index;
        let off = inner_segments.segments[index].append(record)?;

        Ok(off)
    }

    fn read(&mut self, off: u64) -> anyhow::Result<PRecord> {
        let mut inner_segments = self.inner_segments.write().unwrap();
        for segment in &mut inner_segments.segments {
            if segment.base_offset <= off && off < segment.next_offset {
                return segment.read(off);
            }
        }

        Err(anyhow!(format!("offset out of range: {}", off)))
    }

    fn truncate(&mut self, lowest: u64) -> anyhow::Result<()> {
        let mut inner_segments = self.inner_segments.write().unwrap();
        let mut segments = vec![];
        mem::swap(&mut inner_segments.segments, &mut segments);

        for segment in segments {
            if segment.next_offset <= lowest + 1 {
                segment.remove()?;
                continue;
            }
            inner_segments.segments.push(segment);
        }

        Ok(())
    }

    fn remove(self) -> anyhow::Result<()> {
        remove_dir_contents(self.dir)?;
        Ok(())
    }

    fn new_segment(
        dir: &str,
        config: Config,
        segments: &mut InnerSegments,
        off: u64,
    ) -> anyhow::Result<()> {
        let s = Segment::new_segment(dir, off, config)?;

        segments.segments.push(s);
        segments.active_segment_index = segments.segments.len() - 1;

        Ok(())
    }

    fn lowest_offset(&self) -> u64 {
        let inner_segments = self.inner_segments.read().unwrap();

        inner_segments.segments[0].base_offset
    }

    fn highest_offset(&self) -> u64 {
        let inner_segments = self.inner_segments.read().unwrap();

        Log::highest_offset_inner(&inner_segments.segments)
    }

    fn highest_offset_inner(segments: &[Segment]) -> u64 {
        // 絶対にsegmentsに値が入っていることを保証できるかは要確認。
        let off = segments.last().unwrap();
        if off.next_offset == 0 {
            return 0;
        }
        off.next_offset - 1
    }
}

fn remove_dir_contents(path: impl AsRef<Path>) -> anyhow::Result<()> {
    for entry in read_dir(path)? {
        remove_file(entry?.path())?;
    }
    Ok(())
}
