use crate::{
    log::{config::Config, remove_dir_contents, Log},
    server::record::PRecord,
};

use tempfile::tempdir;

#[test]
fn test_append_read() {
    run(append_read);
}

#[test]
fn test_out_of_range_err() {
    run(out_of_range_err);
}

#[test]
fn test_init_existing() {
    run(init_existing);
}

#[test]
fn test_truncate() {
    run(truncate);
}

fn append_read(mut log: Log) {
    let mut record = PRecord {
        value: b"hello, world".to_vec(),
        offset: 0,
    };

    let off = log.append(&mut record).unwrap();

    assert_eq!(0, off);

    let read = log.read(off).unwrap();

    assert_eq!(record.value, read.value);
}

fn out_of_range_err(mut log: Log) {
    let read = log.read(1);

    assert!(read.is_err());
}

fn init_existing(mut log: Log) {
    let mut record = PRecord {
        value: b"hello, world".to_vec(),
        offset: 0,
    };

    for _ in 0..3 {
        log.append(&mut record).unwrap();
    }
    let off = log.lowest_offset();
    assert_eq!(0, off);

    let off = log.highest_offset();
    assert_eq!(2, off);

    let dir = log.dir.clone();
    let config = log.config.clone();

    drop(log);

    let log = Log::new_log(dir, config).unwrap();

    let off = log.lowest_offset();
    assert_eq!(0, off);

    let off = log.highest_offset();
    assert_eq!(2, off);
}

fn truncate(mut log: Log) {
    let mut record = PRecord {
        value: b"hello, world".to_vec(),
        offset: 0,
    };

    for _ in 0..3 {
        log.append(&mut record).unwrap();
    }

    let read = log.read(0);
    assert!(read.is_ok());

    log.truncate(1).unwrap();

    let read = log.read(0);
    assert!(read.is_err());
}

fn run<F: Fn(Log)>(f: F) {
    let dir = tempdir().unwrap();

    let mut config = Config::default();
    config.max_store_bytes = 32;
    let log = Log::new_log(dir.path().to_string_lossy().to_string(), config).unwrap();

    f(log);

    remove_dir_contents(dir).unwrap();
}
