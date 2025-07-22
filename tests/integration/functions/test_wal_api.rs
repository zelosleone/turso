use rusqlite::types::Value;

use crate::common::{limbo_exec_rows, TempDatabase};

#[test]
fn test_wal_frame_count() {
    let db = TempDatabase::new_empty(false);
    let conn = db.connect_limbo();
    assert_eq!(conn.wal_frame_count().unwrap(), 0);
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    assert_eq!(conn.wal_frame_count().unwrap(), 2);
    conn.execute("INSERT INTO t VALUES (10, 10), (5, 1)")
        .unwrap();
    assert_eq!(conn.wal_frame_count().unwrap(), 3);
    conn.execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn.wal_frame_count().unwrap(), 15);
}

#[test]
fn test_wal_frame_transfer_no_schema_changes() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (10, 10), (5, 1)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_frame_count().unwrap(), 15);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    for frame_id in 1..=conn1.wal_frame_count().unwrap() as u32 {
        let c = conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        db1.io.wait_for_completion(c).unwrap();
        conn2.wal_insert_frame(frame_id, &frame).unwrap();
    }
    conn2.wal_insert_end().unwrap();
    assert_eq!(conn2.wal_frame_count().unwrap(), 15);
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, length(y) FROM t"),
        vec![
            vec![Value::Integer(5), Value::Integer(1)],
            vec![Value::Integer(10), Value::Integer(2)],
            vec![Value::Integer(1024), Value::Integer(40960)],
        ]
    );
}

#[test]
fn test_wal_frame_transfer_no_schema_changes_rollback() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_frame_count().unwrap(), 14);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    for frame_id in 1..=(conn1.wal_frame_count().unwrap() as u32 - 1) {
        let c = conn1.wal_get_frame(frame_id, &mut frame).unwrap();
        db1.io.wait_for_completion(c).unwrap();
        conn2.wal_insert_frame(frame_id, &frame).unwrap();
    }
    conn2.wal_insert_end().unwrap();
    assert_eq!(conn2.wal_frame_count().unwrap(), 2);
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, length(y) FROM t"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
    conn2.execute("CREATE TABLE q(x)").unwrap();
    conn2
        .execute("INSERT INTO q VALUES (randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&db2, &conn2, "SELECT x, LENGTH(y) FROM t"),
        vec![] as Vec<Vec<rusqlite::types::Value>>
    );
}

#[test]
fn test_wal_frame_conflict() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE q(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    assert_eq!(conn1.wal_frame_count().unwrap(), 2);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();
    let c = conn1.wal_get_frame(1, &mut frame).unwrap();
    db1.io.wait_for_completion(c).unwrap();
    assert!(conn2.wal_insert_frame(1, &frame).is_err());
}

#[test]
fn test_wal_frame_far_away_write() {
    let db1 = TempDatabase::new_empty(false);
    let conn1 = db1.connect_limbo();
    let db2 = TempDatabase::new_empty(false);
    let conn2 = db2.connect_limbo();
    conn1
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn2
        .execute("CREATE TABLE t(x INTEGER PRIMARY KEY, y)")
        .unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1024, randomblob(4096 * 10))")
        .unwrap();
    assert_eq!(conn1.wal_frame_count().unwrap(), 14);
    let mut frame = [0u8; 24 + 4096];
    conn2.wal_insert_begin().unwrap();

    let c = conn1.wal_get_frame(3, &mut frame).unwrap();
    db1.io.wait_for_completion(c).unwrap();
    conn2.wal_insert_frame(3, &frame).unwrap();

    let c = conn1.wal_get_frame(5, &mut frame).unwrap();
    db1.io.wait_for_completion(c).unwrap();
    assert!(conn2.wal_insert_frame(5, &frame).is_err());
}
