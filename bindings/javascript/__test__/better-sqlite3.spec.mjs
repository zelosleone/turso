import test from "ava";

import Database from "better-sqlite3";

test("Open in-memory database", async (t) => {
    const [db] = await connect(":memory:");
    t.is(db.memory, true);
});

test("Statement.get()", async (t) => {
    const [db] = await connect(":memory:");
    const stmt = db.prepare("SELECT 1");
    const result = stmt.get();
    t.is(result["1"], 1);
});

const connect = async (path) => {
    const db = new Database(path);
    return [db];
};
