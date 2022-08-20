use std::fmt::format;
use std::fs::remove_file;
use std::path::PathBuf;
use std::{fs::OpenOptions, path::Path};

use crate::server::deserialize_recored;
use crate::server::serialize_record;

use crate::server::record::PRecord;

use super::{config::Config, index::Index, store::Store};

pub mod record {
    tonic::include_proto!("log.v1");
}

pub struct Segment {
    pub store: Store,
    index: Index,
    pub base_offset: u64,
    pub next_offset: u64,
    config: Config,
    store_path: PathBuf,
    index_path: PathBuf,
}

impl Segment {
    pub fn new_segment(
        dir: impl AsRef<Path>,
        base_offset: u64,
        config: Config,
    ) -> anyhow::Result<Self> {
        let store_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(dir.as_ref().join(format!("{}.store", base_offset)))?;

        let store = Store::new(store_file)?;

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.as_ref().join(format!("{}.index", base_offset)))?;

        let mut index = Index::new_index(index_file, &config)?;

        let next_offset;

        if let Ok((off, _)) = index.read(-1) {
            next_offset = base_offset + off as u64 + 1;
        } else {
            next_offset = base_offset;
        }

        Ok(Segment {
            store,
            index,
            base_offset,
            next_offset,
            config,
            store_path: dir.as_ref().join(format!("{}.store", base_offset)),
            index_path: dir.as_ref().join(format!("{}.index", base_offset)),
        })
    }

    pub fn append(&mut self, record: &mut PRecord) -> anyhow::Result<u64> {
        let cur = self.next_offset;
        record.offset = cur;

        let (_, pos) = self.store.append(serialize_record(record))?;

        self.index
            .write((self.next_offset - self.base_offset).try_into()?, pos)?;

        self.next_offset += 1;

        Ok(cur)
    }

    pub fn read(&mut self, off: u64) -> anyhow::Result<PRecord> {
        let (_, pos) = self.index.read((off - self.base_offset).try_into()?)?;

        let p = self.store.read(pos)?;

        let record = deserialize_recored(p)?;

        Ok(record)
    }

    pub fn is_maxed(&self) -> anyhow::Result<bool> {
        Ok(self.store.size >= self.config.max_store_bytes
            || self.index.size >= self.config.max_index_bytes
            || self.index.is_maxed()?)
    }

    pub fn remove(self) -> anyhow::Result<()> {
        remove_file(&self.store_path)?;
        remove_file(&self.index_path)?;

        Ok(())
    }
}
