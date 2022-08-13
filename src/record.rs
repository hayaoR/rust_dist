use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Serialize, Deserialize, Clone)]
pub struct Record {
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
    offset: Option<usize>,
}

pub struct Log {
    records: Mutex<Vec<Record>>,
}

impl Log {
    pub fn new() -> Self {
        Log {
            records: Mutex::new(Vec::new()),
        }
    }

    pub fn append(&self, mut record: Record) -> anyhow::Result<usize> {
        let mut records = self.records.lock().unwrap();
        let length = records.len();
        record.offset = Some(length);
        records.push(record);

        Ok(length)
    }

    pub fn read(&self, offset: usize) -> anyhow::Result<Record> {
        let records = self.records.lock().unwrap();
        if offset >= records.len() {
            return Err(anyhow::anyhow!("Err offset not found"));
        }
        Ok(records[offset].clone())
    }
}

impl Default for Log {
    fn default() -> Self {
        Self::new()
    }
}