use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::{fs::File, sync::Mutex};

pub const LEN_WIDTH: u64 = 8;

pub struct Store {
    file: Mutex<File>,
    size: u64,
}

impl Store {
    pub fn new(f: File) -> anyhow::Result<Self> {
        let metadata = f.metadata()?;

        Ok(Store {
            file: Mutex::new(f),
            size: metadata.len(),
        })
    }

    pub fn append(&mut self, p: &[u8]) -> anyhow::Result<(u64, u64)> {
        let mut f = self.file.lock().unwrap();
        let pos = self.size;

        f.write_u64::<BigEndian>(p.len() as u64)?;

        f.write(p)?;

        self.size += p.len() as u64 + LEN_WIDTH;

        Ok((p.len() as u64 + LEN_WIDTH, pos))
    }

    pub fn read(&mut self, pos: u64) -> anyhow::Result<Vec<u8>> {
        let mut f = self.file.lock().unwrap();

        f.seek(SeekFrom::Start(pos))?;
        let size = f.read_u64::<BigEndian>()?;

        let mut buf = vec![0; size as usize];
        f.seek(SeekFrom::Start(pos + LEN_WIDTH))?;
        f.read_exact(&mut buf)?;

        Ok(buf)
    }
}
