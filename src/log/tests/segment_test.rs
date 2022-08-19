use tempfile::tempdir;

use crate::log::config::Config;
use crate::log::index::ENT_WIDTH;
use crate::log::segment::Segment;
use crate::log::store::LEN_WIDTH;
use crate::server::record::PRecord;
use crate::server::serialize_record;

#[test]
fn test_read_write() {
    let dir = tempdir().unwrap();

    let config = Config {
        max_store_bytes: 1024,
        max_index_bytes: ENT_WIDTH * 3,
        initial_offset: 0,
    };

    let mut record = PRecord::default();
    record.value = b"hello world".to_vec();

    {
        let mut s = Segment::new_segment(&dir, 16, config).unwrap();
        assert_eq!(s.next_offset, 16);
        assert!(!s.is_maxed().unwrap());

        for i in 0..3 {
            let off = s.append(&mut record).unwrap();
            assert_eq!(16 + i, off);

            let got = s.read(off).unwrap();
            assert_eq!(record.value, got.value);
        }

        let e = s.append(&mut record);
        assert!(e.is_err());

        assert!(s.is_maxed().unwrap());
    }

    let p = serialize_record(&record);

    let config = Config {
        max_store_bytes: (TryInto::<u64>::try_into(p.len()).unwrap() + LEN_WIDTH) * 4,
        max_index_bytes: 1024,
        initial_offset: 0,
    };

    let s = Segment::new_segment(&dir, 16, config.clone()).unwrap();

    assert!(s.is_maxed().unwrap());

    s.remove().unwrap();

    let s = Segment::new_segment(&dir, 16, config.clone()).unwrap();
    assert!(!s.is_maxed().unwrap());
}
