import test from "ava";

import Database from "better-sqlite3";

test("Open in-memory database", async (t) => {
    const [db] = await connect(":memory:");
    t.is(db.memory, true);
});


test("Statement.get() returns data", async (t) => {
    const [db] = await connect(":memory:");
    const stmt = db.prepare("SELECT 1");
    const result = stmt.get();
    t.is(result["1"], 1);
    const result2 = stmt.get();
    t.is(result2["1"], 1);
});

test("Statement.get() returns undefined when no data", async (t) => {
    const [db] = await connect(":memory:");
    const stmt = db.prepare("SELECT 1 WHERE 1 = 2");
    const result = stmt.get();
    t.is(result, undefined);
});

const connect = async (path) => {
    const db = new Database(path);
    return [db];
};
