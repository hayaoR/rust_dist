use prost::Message;
use std::io::Cursor;

use crate::server::record::PRecord;

pub mod record {
    tonic::include_proto!("log.v1");
}

pub fn serialize_record(record: &PRecord) -> Vec<u8> {
    let mut buf = Vec::with_capacity(record.encoded_len());
    record.encode(&mut buf).unwrap();
    buf
}

pub fn deserialize_recored(buf: Vec<u8>) -> Result<PRecord, prost::DecodeError> {
    PRecord::decode(&mut Cursor::new(buf))
}
