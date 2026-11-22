use super::*;
use crate::model::{Counters, Info, Report, Totals};

fn report(warning: String) -> Event {
    Event::Report(Box::new(Report {
        index_generation: 1,
        mode: "full".to_owned(),
        reason: "checkpoint encoding test".to_owned(),
        completed_at_unix_ms: 0,
        complete: true,
        settled: true,
        totals: Totals::default(),
        counters: Counters::default(),
        audit: None,
        warning: Some(warning),
    }))
}

#[test]
fn reusable_encoder_preserves_the_existing_wire_format() {
    use std::os::unix::ffi::OsStrExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("tree");
    fs::create_dir(&root).unwrap();
    let cache = Cache::new(&root, Some(&temp.path().join("cache")), false).unwrap();
    let _lock = cache.lock().unwrap();
    let loaded = load(&cache).unwrap();
    let mut writer = Journal::open(&cache, &loaded).unwrap();
    let info = Info::from(&fs::symlink_metadata(&root).unwrap());
    for event in [
        Event::Start {
            root: root.as_os_str().as_bytes().to_vec(),
            info,
        },
        report("x".repeat(16_000)),
        Event::Begin { directory: 1, info },
    ] {
        let expected = codec().serialize(&event).unwrap();
        let offset = writer.offset;
        writer.append(&event).unwrap();
        let length =
            u32::from_le_bytes(writer.map[offset..offset + 4].try_into().unwrap()) as usize;
        assert_eq!(length, expected.len());
        assert_eq!(
            &writer.map[offset + FRAME_HEADER..offset + FRAME_HEADER + length],
            expected
        );
    }
    assert_eq!(load(&cache).unwrap().sequence, 3);
}

#[test]
fn oversized_encoding_does_not_publish_a_partial_frame() {
    let temp = tempfile::tempdir().unwrap();
    let cache = Cache::new(temp.path(), Some(&temp.path().join("cache")), false).unwrap();
    let _lock = cache.lock().unwrap();
    let loaded = load(&cache).unwrap();
    let mut writer = Journal::open(&cache, &loaded).unwrap();
    assert!(writer.append(&report("x".repeat(SEGMENT_BYTES))).is_err());
    assert_eq!(writer.sequence, 0);
    assert_eq!(&writer.map[HEADER..HEADER + 4], &[0; 4]);
}
