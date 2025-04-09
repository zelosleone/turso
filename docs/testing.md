# Testing in Limbo

Limbo supports a comprehensive testing system to ensure correctness, performance, and compatibility with SQLite.

## 1. Compatibility Tests

The `make test` target is the main entry point.

Most compatibility tests live in the testing/ directory and are written in SQLite’s TCL test format. These tests ensure that Limbo matches SQLite’s behavior exactly. The database used during these tests is located at testing/testing.db, which includes the following schema:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone_number TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    age INTEGER
);
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT,
    price REAL
);
CREATE INDEX age_idx ON users (age);
```

You can freely write queries against these tables during compatibility testing.

### Shell and Python-based Tests

For cases where output or behavior differs intentionally from SQLite (e.g. due to new features or limitations), tests should be placed in the testing/cli_tests/ directory and written in Python.

These tests use the TestLimboShell class:

```python
from cli_tests.common import TestLimboShell

def test_uuid():
    limbo = TestLimboShell()
    limbo.run_test_fn("SELECT uuid4_str();", lambda res: len(res) == 36)
    limbo.quit()
```

You can use run_test, run_test_fn, or debug_print to interact with the shell and validate results. 
The constructor takes an optional argument with the `sql` you want to initiate the tests with.  You can also enable blob testing or override the executable and flags.

Use these Python-based tests for validating:

  - Output formatting

  - Shell commands and .dot interactions

  - Limbo-specific extensions in `testing/cli_tests/extensions.py`

  - Any known divergence from SQLite behavior


> Logging and Tracing
If you wish to trace internal events during test execution, you can set the RUST_LOG environment variable before running the test. For example:

```bash
RUST_LOG=none,limbo_core=trace make test
```

This will enable trace-level logs for the limbo_core crate and disable logs elsewhere. Logging all internal traces to the `testing/test.log` file. 

**Note:** trace logs can be very verbose—it's not uncommon for a single test run to generate megabytes of logs.


## Deterministic Simulation Testing (DST):

TODO!


## Fuzzing

TODO!



