use crate::log::store::{Store, LEN_WIDTH};

const ARR: &[u8] = b"hello, world!";
const WIDTH: u64 = ARR.len() as u64 + LEN_WIDTH;

#[test]
fn test_store_append_read() {
    let f = tempfile::tempfile().unwrap();

    let mut store = Store::new(f).unwrap();

    test_append(&mut store);
    test_read(&mut store);
}

fn test_append(store: &mut Store) {
    for i in 1..4 {
        let (n, pos) = store.append(ARR.to_vec()).unwrap();
        assert_eq!(pos + n, WIDTH * i);
    }
}

fn test_read(store: &mut Store) {
    let mut pos = 0;
    for i in 1..4 {
        let data = store.read(pos).unwrap();
        assert_eq!(ARR.to_vec(), data, "{}回目", i);
        pos += WIDTH;
    }
}
