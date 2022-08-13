use std::fs::File;
use std::io::{Error, ErrorKind, SeekFrom, Seek};
use byteorder::{BigEndian};
use byteorder::{WriteBytesExt, ReadBytesExt};

use super::config::Config;

const OFF_WIDTH : u64 = 4;
const POS_WIDTH : u64 = 8;
const ENT_WIDTH : u64 = OFF_WIDTH + POS_WIDTH;

pub struct Index {
    file: File,
    size: u64,
}

impl Index {
    pub fn new_index(f: File, c: &Config) -> anyhow::Result<Self> {
        let metadata = f.metadata()?;
        let size = metadata.len();

        f.set_len(c.segment.max_index_bytes)?;

        Ok(Index {
            file: f,
            size,
        })
    }

    pub fn read(&mut self, index: u32) -> anyhow::Result<(u32, u64)> {
        if self.size == 0 {
            return Ok((0, 0));
        }

        let pos = index as u64 * ENT_WIDTH;

        if self.size < pos {
            return Err(From::from(Error::from(ErrorKind::UnexpectedEof)));
        }

        self.file.seek(SeekFrom::Start(pos))?;
        let out = self.file.read_u32::<BigEndian>()?;

        self.file.seek(SeekFrom::Start(pos + OFF_WIDTH))?;
        let pos = self.file.read_u64::<BigEndian>()?;

        Ok((out, pos))
    }

    pub fn write(&mut self, off: u32, pos: u64) -> anyhow::Result<()> {
        if self.is_maxed()? {
            return Err(From::from(Error::from(ErrorKind::UnexpectedEof)));
        }

        self.file.seek(SeekFrom::Start(self.size))?;
        self.file.write_u32::<BigEndian>(off)?;
        self.file.write_u64::<BigEndian>(pos)?;

        self.size += ENT_WIDTH;

        Ok(())
    }

    fn is_maxed(&self) -> anyhow::Result<bool> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len() < self.size + ENT_WIDTH)
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        // drop should not be panic. so we ignore error.
        let _ = self.file.set_len(self.size);
    }
}
