# waveQl Test Environment

This is a demonstration of the **waveQl** SQL query builder. It shows how waveQl separates logical field names (used in your code) from actual database column names.

## Quick Start (Recommended)

1. **Place waveQl files** – Copy all waveQl PHP files into the parent directory of this test folder (one level up).
   Required files:
   `waveQl.php`, `waveQlCore.php`, `waveQlRead.php`, `waveQlWrite.php`,
   `waveQlException.php`, `waveQlDbInterface.php`, `dbAdapterMysqli.php`.

2. **Edit database config** – In `config.php`, set your MySQL credentials (host, username, password, database name).
   The database name will be created if it doesn't exist.

3. **Auto‑init database** – Open `test.php?initSQL=1` in your browser. This will:
   - Read the SQL statements from `setup.sql`
   - Drop existing tables `continents` and `countries` (if they exist)
   - Create fresh tables and fill them with 32 interesting countries
   - Show a success message.

4. **Start testing** – After initialisation, use the form to run SELECT queries (with operators like `>10`, `~text~`, etc.) or INSERT new countries.

If you ever need to reset the database, just open `test.php?initSQL=1` again.

## How waveQl Works

- **Read**: Provide an array of filter values (e.g., `['Population' => '>1000000']`) and optional meta (sort, pagination, search). waveQl builds a secure SQL query (prepared statements) and returns the result.
- **Write**: Use `setMeta(['uniqueKey' => 'id'])` and `setValues([...])`. If the `uniqueKey` field is present in the values, it's an UPDATE; otherwise INSERT. The `returning` option returns the full record.

All operators: `>`, `<`, `!`, `~`, `NULL`, `BLANK`, `EMPTY`, ranges (`10><20`, …) work as documented.

The test form uses the `waveQl` factory class under the namespace `\e2\waveQl`.