# waveQl Test Environment

This is a demonstration of the **waveQl** SQL query generator. It shows how waveQl separates logical field names (used in your code) from the actual database column names.

###### Quick Start (Recommended)

### 1. **Place the waveQl files** – The waveQl PHP files should be located in the parent directory in a "src" folder (exactly the structure shown here).

Required files:
`waveQl.php`, `waveQlCore.php`, `waveQlRead.php`, `waveQlWrite.php`,
`waveQlException.php`, `waveQlDbInterface.php`, `dbAdapterMysqli.php`


### 2. **Edit the database configuration** – Enter your MySQL access credentials (host, username, password, database name) in the `config.php` file.

The database will be created if it doesn't already exist.



### 3. **Automatically Initialize the Database** – Open `test.php?initSQL=1` in your browser. This will execute the following steps:

- The SQL statements from `setup.sql` will be read.
- Existing `continents` and `countries` tables will be deleted (if they exist).
- New tables will be created and populated with 32 countries of interest.
- A success message will be displayed.


### 4. **Testing** – After initialization, you can use the form to execute SELECT queries (with operators such as `>10`, `~text~`, etc.) or add new countries.

If you need to reset the database, simply open `test.php?initSQL=1` again.



## How waveQl Works

- **Reading**: Specify an array of filter values (e.g., `['Population' => '>1000000']`) and optional metadata (sorting, pagination, search). waveQl creates a secure SQL query (prepared statements) and returns the result.

- **Writing**: Use `setMeta(['uniqueKey' => 'id'])` and `setValues([...])`. If the `uniqueKey` field is present in the values, it is an UPDATE; otherwise, it is an INSERT. The `returning` option returns the complete record.

All operators: `>`, `<`, `!`, `~`, `NULL`, `BLANK`, `EMPTY`, ranges (`10><20`, ...) function as documented.

The test form uses the factory class `waveQl` in the namespace `\e2\`.