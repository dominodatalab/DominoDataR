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
result <- query(client, "my_datasource", "SELECT * FROM my_schema.my_table")
df <- as.data.frame(result)
```

### Writing data

``` r
df <- data.frame(
  id       = 1:3,
  name     = c("Alice", "Bob", "Charlie"),
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

`Date` columns are stored as DB2 `DATE` (not `TIMESTAMP`) and round-trip
without any timezone shift, regardless of the session timezone.

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

## Migrating from the Old API

If your code uses `DominoDataSourceWrite` or `DominoDataSourceQuery`, those
functions are from an older version of DominoDataR and are no longer available.
Replace them as follows:

### Reading

```r
# Old
df <- DominoDataSourceQuery("MY_DATASOURCE", "SELECT * FROM my_schema.my_table")

# New
client <- datasource_client()
df <- as.data.frame(query(client, "MY_DATASOURCE", "SELECT * FROM my_schema.my_table"))
```

### Writing

```r
# Old
DominoDataSourceWrite("MY_DATASOURCE", "MY_SCHEMA.MY_TABLE", df,
                      if_table_exists = "replace")

# New
client <- datasource_client()
write_dataframe(client, "MY_DATASOURCE", "MY_SCHEMA.MY_TABLE", df,
                if_table_exists = "replace")
```

The `client` object can be created once and reused across multiple calls in the
same session.

### Complete example (equivalent to old mtcars pattern)

```r
library(DominoDataR)
library(dplyr)

client <- datasource_client()

# Prepare data
mtcars_df <- mtcars %>%
  mutate(
    var_date     = lubridate::today(),   # stored as DB2 DATE
    var_datetime = lubridate::now(),     # stored as DB2 TIMESTAMP
    var_factor   = factor("A")
  )

# Write
write_dataframe(client, "MY_DATASOURCE", "MY_SCHEMA.MTCARS",
                mtcars_df, if_table_exists = "replace")

# Read back
result <- as.data.frame(query(client, "MY_DATASOURCE",
                              "SELECT * FROM MY_SCHEMA.MTCARS"))
```
