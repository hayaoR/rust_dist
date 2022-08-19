use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Serialize, Deserialize, Clone)]
pub struct Record {
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
    pub offset: Option<u64>,
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

    pub fn append(&self, mut record: Record) -> anyhow::Result<u64> {
        let mut records = self.records.lock().unwrap();
        let length = records.len().try_into()?;
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
