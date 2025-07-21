use std::sync::Arc;

use crate::common::TempDatabase;

#[test]
fn test_schema_change() {
    let tmp_db = TempDatabase::new_empty(false);
    let conn1 = tmp_db.connect_limbo();
    conn1.execute("CREATE TABLE t(x, y, z)").unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1, 2, 3), (10, 20, 30)")
        .unwrap();
    let conn2 = tmp_db.connect_limbo();
    let mut stmt = conn2.prepare("SELECT x, z FROM t").unwrap();
    conn1.execute("ALTER TABLE t DROP COLUMN x").unwrap();
    let row = loop {
        match stmt.step() {
            Ok(turso_core::StepResult::Row) => {
                let row = stmt.row().unwrap();
                break row;
            }
            Ok(turso_core::StepResult::IO) => {
                stmt.run_once().unwrap();
            }
            _ => panic!("unexpected step result"),
        }
    };
    println!("{:?} {:?}", row.get_value(0), row.get_value(1));
}

#[test]
fn test_create_multiple_connections() -> anyhow::Result<()> {
    let tries = 10;
    for _ in 0..tries {
        let tmp_db = Arc::new(TempDatabase::new_empty(false));
        {
            let conn = tmp_db.connect_limbo();
            conn.execute("CREATE TABLE t(x)").unwrap();
        }

        let mut threads = Vec::new();
        for i in 0..10 {
            let tmp_db_ = tmp_db.clone();
            threads.push(std::thread::spawn(move || {
                let conn = tmp_db_.connect_limbo();
                conn.execute(format!("INSERT INTO t VALUES ({i})").as_str())
                    .unwrap();
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }

        let conn = tmp_db.connect_limbo();
        let mut stmt = conn.prepare("SELECT * FROM t").unwrap();
        let mut rows = Vec::new();
        while matches!(stmt.step().unwrap(), turso_core::StepResult::Row) {
            let row = stmt.row().unwrap();
            rows.push(row.get::<i64>(0).unwrap());
        }
        rows.sort();
        assert_eq!(rows, (0..10).collect::<Vec<_>>());
    }
    Ok(())
}
