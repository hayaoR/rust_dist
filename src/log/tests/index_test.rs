use std::fs::OpenOptions;

use crate::log::{
    config::{Config, Segment},
    index::Index,
};
use tempfile::tempdir;

#[test]
fn test_read_write() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("my_temporary");

    let config = Config {
        segment: Segment {
            max_store_bytes: 0,
            max_index_bytes: 1024,
            initial_offset: 0,
        },
    };

    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .unwrap();

        let mut index = Index::new_index(file, &config).unwrap();

        let entries = vec![(0, 7), (1, 10)];

        for entry in &entries {
            index.write(entry.0, entry.1).unwrap();

            let (_, pos) = index.read(entry.0).unwrap();

            assert_eq!(pos, entry.1);
        }
    }

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let mut index = Index::new_index(file, &config).unwrap();

    let entries = vec![(0, 7), (1, 10)];

    for entry in &entries {
        // index.write(entry.0, entry.1).unwrap();

        let (_, pos) = index.read(entry.0).unwrap();

        assert_eq!(pos, entry.1);
    }
}
