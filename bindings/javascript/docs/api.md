# class Database

The `Database` class represents a connection that can prepare and execute SQL statements.

## Methods

### new Database(path, [options]) ⇒ Database

Creates a new database connection.

| Param   | Type                | Description               |
| ------- | ------------------- | ------------------------- |
| path    | <code>string</code> | Path to the database file |
| options | <code>object</code> | Options.                  |

The `path` parameter points to the SQLite database file to open. If the file pointed to by `path` does not exists, it will be created.
To open an in-memory database, please pass `:memory:` as the `path` parameter.

The function returns a `Database` object.

### prepare(sql) ⇒ Statement

Prepares a SQL statement for execution.

| Param  | Type                | Description                          |
| ------ | ------------------- | ------------------------------------ |
| sql    | <code>string</code> | The SQL statement string to prepare. |

The function returns a `Statement` object.

This function is currently not supported.

### transaction(function) ⇒ function

Returns a function that runs the given function in a transaction.

| Param    | Type                  | Description                           |
| -------- | --------------------- | ------------------------------------- |
| function | <code>function</code> | The function to run in a transaction. |

This function is currently not supported.

### pragma(string, [options]) ⇒ results

This function is currently not supported.

### backup(destination, [options]) ⇒ promise

This function is currently not supported.

### serialize([options]) ⇒ Buffer

This function is currently not supported.

### function(name, [options], function) ⇒ this

This function is currently not supported.

### aggregate(name, options) ⇒ this

This function is currently not supported.

### table(name, definition) ⇒ this

This function is currently not supported.

### loadExtension(path, [entryPoint]) ⇒ this

Loads a SQLite3 extension

This function is currently not supported.

### exec(sql) ⇒ this

Executes a SQL statement.

| Param  | Type                | Description                          |
| ------ | ------------------- | ------------------------------------ |
| sql    | <code>string</code> | The SQL statement string to execute. |

This function is currently not supported.

### interrupt() ⇒ this

Cancel ongoing operations and make them return at earliest opportunity.

**Note:** This is an extension in libSQL and not available in `better-sqlite3`.

This function is currently not supported.

### close() ⇒ this

Closes the database connection.

This function is currently not supported.

# class Statement

## Methods

### run([...bindParameters]) ⇒ object

Executes the SQL statement and returns an info object.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

The returned info object contains two properties: `changes` that describes the number of modified rows and `info.lastInsertRowid` that represents the `rowid` of the last inserted row.

This function is currently not supported.

### get([...bindParameters]) ⇒ row

Executes the SQL statement and returns the first row.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

This function is currently not supported.

### all([...bindParameters]) ⇒ array of rows

Executes the SQL statement and returns an array of the resulting rows.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

This function is currently not supported.

### iterate([...bindParameters]) ⇒ iterator

Executes the SQL statement and returns an iterator to the resulting rows.

| Param          | Type                          | Description                                      |
| -------------- | ----------------------------- | ------------------------------------------------ |
| bindParameters | <code>array of objects</code> | The bind parameters for executing the statement. |

This function is currently not supported.

### pluck([toggleState]) ⇒ this

This function is currently not supported.

### expand([toggleState]) ⇒ this

This function is currently not supported.

### raw([rawMode]) ⇒ this

Toggle raw mode.

| Param   | Type                 | Description                                                                       |
| ------- | -------------------- | --------------------------------------------------------------------------------- |
| rawMode | <code>boolean</code> | Enable or disable raw mode. If you don't pass the parameter, raw mode is enabled. |

This function enables or disables raw mode. Prepared statements return objects by default, but if raw mode is enabled, the functions return arrays instead.

This function is currently not supported.

### columns() ⇒ array of objects

Returns the columns in the result set returned by this prepared statement.

This function is currently not supported.

### bind([...bindParameters]) ⇒ this

This function is currently not supported.
