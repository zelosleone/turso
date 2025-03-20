import sqlite3

import pytest

import limbo


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_fetchall_select_all_users(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    users = cursor.fetchall()
    assert users
    assert users == [(1, "alice"), (2, "bob")]


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_fetchall_select_user_ids(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users")

    user_ids = cursor.fetchall()
    assert user_ids
    assert user_ids == [(1,), (2,)]


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_in_memory_fetchone_select_all_users(provider):
    conn = connect(provider, ":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT)")
    cursor.execute("INSERT INTO users VALUES (1, 'alice')")

    cursor.execute("SELECT * FROM users")

    alice = cursor.fetchone()
    assert alice
    assert alice == (1, "alice")


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_fetchone_select_all_users(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    alice = cursor.fetchone()
    assert alice
    assert alice == (1, "alice")

    bob = cursor.fetchone()
    assert bob
    assert bob == (2, "bob")


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_fetchone_select_max_user_id(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM users")

    max_id = cursor.fetchone()
    assert max_id
    assert max_id == (2,)

# Test case for: https://github.com/tursodatabase/limbo/issues/494
@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_commit(provider):
    con = limbo.connect("sqlite.db")
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT (datetime('now'))
        )
    """)

    con.commit()

    sample_users = [
        ("alice", "alice@example.com", "admin"),
        ("bob", "bob@example.com", "user"),
        ("charlie", "charlie@example.com", "moderator"),
        ("diana", "diana@example.com", "user")
    ]

    for username, email, role in sample_users:
        cur.execute("INSERT INTO users (username, email, role) VALUES (?, ?, ?)", (username, email, role))

    con.commit()

    # Now query the table
    res = cur.execute("SELECT * FROM users")
    record = res.fetchone()
    assert record

def connect(provider, database):
    if provider == "limbo":
        return limbo.connect(database)
    if provider == "sqlite3":
        return sqlite3.connect(database)
    raise Exception(f"Provider `{provider}` is not supported")
