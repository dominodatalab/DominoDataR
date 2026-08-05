# DominoDataR

<!-- badges: start -->
[![R-CMD-check](https://github.com/dominodatalab/DominoDataR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/dominodatalab/DominoDataR/actions/workflows/R-CMD-check.yaml)
![cran-check](https://badges.cranchecks.info/summary/DominoDataR.svg)
<!-- badges: end -->

Domino Data API for interacting with Domino Data Sources from R.

## Installation

### Via CRAN

``` r
install.packages("DominoDataR")
```

### Via 'remotes' or 'devtools'

``` r
install.packages("remotes")
remotes::install_github("dominodatalab/DominoDataR")
```

## Prerequisite

DominoDataR depends on the [Python domino-data library](https://pypi.org/project/dominodatalab-data/).
Install it with the included helper:

``` r
DominoDataR::py_domino_data_install()
```

## Quick Start

All operations start with a client object created once per session:

``` r
library(DominoDataR)

client <- datasource_client()
```

### Reading data

``` r
# Returns an Arrow Table — use as.data.frame() or dplyr directly
df <- as.data.frame(query(client, "my_datasource", "SELECT * FROM my_schema.my_table"))
```

### Writing data

``` r
df <- data.frame(
  id        = 1:3,
  name      = c("Alice", "Bob", "Charlie"),
  join_date = as.Date(c("2024-01-01", "2024-03-15", "2024-06-01"))
)

write_dataframe(client, "my_datasource", "MY_SCHEMA.MY_TABLE", df)
```

The `if_table_exists` parameter controls what happens when the table already exists:

| Value | Behaviour |
|-------|-----------|
| `"fail"` | Raise an error (default) |
| `"replace"` | Drop and recreate the table |
| `"truncate"` | Empty the table and refill it |
| `"append"` | Add rows to the existing table |

``` r
# Overwrite the table each run
write_dataframe(client, "my_datasource", "MY_SCHEMA.MY_TABLE", df,
                if_table_exists = "replace")

# Append new rows
write_dataframe(client, "my_datasource", "MY_SCHEMA.MY_TABLE", new_rows,
                if_table_exists = "append")
```

### DDL and DML (DB2 native)

Use `execute_statement()` for statements that return no rows (CREATE, DROP,
INSERT, UPDATE, DELETE, MERGE, TRUNCATE):

``` r
execute_statement(client, "my_datasource",
                  "DELETE FROM MY_SCHEMA.MY_TABLE WHERE status = 'expired'")
```

## Upgrading from an Older Version

If your code uses `DominoDataSourceQuery` or `DominoDataSourceWrite`, those
functions are from an older version of DominoDataR and are no longer available.
The current package uses `query()` and `write_dataframe()` with an explicit
`client` object.

### Step 1 — Update the package

``` r
install.packages("remotes")
remotes::install_github("dominodatalab/DominoDataR")
DominoDataR::py_domino_data_install()
```

### Step 2 — Update your code

**Reading data:**

``` r
# Old (no longer works)
df <- DominoDataSourceQuery("MY_DATASOURCE", "SELECT * FROM my_schema.my_table")

# New
library(DominoDataR)
client <- datasource_client()
df <- as.data.frame(query(client, "MY_DATASOURCE", "SELECT * FROM my_schema.my_table"))
```

**Writing data:**

``` r
# Old (no longer works)
DominoDataSourceWrite("MY_DATASOURCE", "MY_SCHEMA.MY_TABLE", df,
                      if_table_exists = "replace")

# New
library(DominoDataR)
client <- datasource_client()
write_dataframe(client, "MY_DATASOURCE", "MY_SCHEMA.MY_TABLE", df,
                if_table_exists = "replace")
```

The `client` object can be created once per session and reused across multiple
read and write calls.

### Complete example

``` r
library(DominoDataR)

client <- datasource_client()

# Write
df <- data.frame(
  id        = 1:3,
  name      = c("Alice", "Bob", "Charlie"),
  join_date = as.Date(c("2024-01-01", "2024-03-15", "2024-06-01"))
)
write_dataframe(client, "MY_DATASOURCE", "MY_SCHEMA.MY_TABLE", df,
                if_table_exists = "replace")

# Read back
df_back <- as.data.frame(query(client, "MY_DATASOURCE",
                               "SELECT * FROM MY_SCHEMA.MY_TABLE"))
```

### Note on R `Date` columns and DB2

R `Date` columns are written as DB2 `DATE` (not `TIMESTAMP`). This means
dates round-trip correctly regardless of the session timezone — there is no
`+2h` shift in CEST or other UTC-offset sessions.

If you have an existing table where a `Date` column was written as DB2
`TIMESTAMP` by an older version of the package, rewrite it with
`if_table_exists = "replace"` to fix the schema automatically:

``` r
write_dataframe(client, "MY_DATASOURCE", "MY_SCHEMA.MY_TABLE", df,
                if_table_exists = "replace")
```

## Column Type Mapping

R types are mapped to database types automatically:

| R type | DB2 type |
|--------|----------|
| `integer` | `INTEGER` |
| `numeric` | `DOUBLE` |
| `character` | `VARCHAR` |
| `logical` | `SMALLINT` (0/1) |
| `Date` | `DATE` |
| `POSIXct` / `POSIXlt` | `TIMESTAMP` |
| `factor` | `VARCHAR` |

## DB2 Native vs Legacy Starburst/Trino

DominoDataR supports two DB2 connector types:

| Connector | Datasource type | Use |
|-----------|----------------|-----|
| Native DB2 | `DB2NativeConfig` | Recommended. Full DB2 SQL dialect, faster reads/writes. |
| Legacy Starburst/Trino | `DB2Config` | Supported but limited. Some DB2-specific SQL syntax requires `passthrough_query()`. |

For the legacy connector with complex DB2 syntax:

``` r
result <- passthrough_query(
  client, "my_starburst_datasource",
  "SELECT DAYOFWEEK(hire_date), COUNT(*) FROM staff GROUP BY ROLLUP(DAYOFWEEK(hire_date))"
)
```

## Troubleshooting

### `Error while loading conda entry point: conda-libmamba-solver`

```
Error while loading conda entry point: conda-libmamba-solver
(module 'libmambapy' has no attribute 'QueryFormat')
```

This is a version mismatch between `libmambapy` and `conda-libmamba-solver` in
the compute environment. It prevents the Python backend from loading, which
means DominoDataR cannot run at all.

**Fix** — run this once in a terminal in your Domino workspace:

``` bash
conda config --set solver classic
```

Then restart your R session. If you do not have terminal access, contact your
Domino platform administrator to fix the base environment image.

### `could not find function "DominoDataSourceQuery"`

You are running an older version of DominoDataR. Follow the steps in
[Upgrading from an Older Version](#upgrading-from-an-older-version) above.

### `could not find function "DominoDataSourceWrite"`

Same as above — see [Upgrading from an Older Version](#upgrading-from-an-older-version).
