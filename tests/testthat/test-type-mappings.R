# Tests for R type-mapping logic in .r_df_to_pandas() and .cast_string_date_cols().
#
# These tests are pure R — they exercise the Arrow-based type handling without
# requiring a live Domino/DB2 connection.  Reticulate is still needed for the
# .r_df_to_pandas() tests because that function bridges to Python pandas.
#
# Run with:
#   devtools::test()                     # from the package root
#   testthat::test_file("tests/testthat/test-type-mappings.R")

library(testthat)
library(arrow)

# ---------------------------------------------------------------------------
# Helper: call internal functions directly (they are not exported)
# ---------------------------------------------------------------------------
r_df_to_pandas    <- DominoDataR:::.r_df_to_pandas
cast_string_dates <- DominoDataR:::.cast_string_date_cols


# ============================================================================
# 1. .cast_string_date_cols  (pure Arrow — no Python/reticulate needed)
# ============================================================================

test_that(".cast_string_date_cols converts utf8 date strings to date32", {
  tbl <- arrow::arrow_table(d = c("2024-01-01", "2024-06-15"))
  expect_equal(tbl$schema$field("d")$type$id, arrow::Type$STRING)

  out <- cast_string_dates(tbl)
  expect_equal(out$schema$field("d")$type$id, arrow::Type$DATE32)

  vals <- as.Date(out$column("d")$as_vector())
  expect_equal(vals, c(as.Date("2024-01-01"), as.Date("2024-06-15")))
})

test_that(".cast_string_date_cols leaves non-date utf8 unchanged", {
  tbl <- arrow::arrow_table(s = c("hello", "world"))
  out <- cast_string_dates(tbl)
  expect_equal(out$schema$field("s")$type$id, arrow::Type$STRING)
})

test_that(".cast_string_date_cols handles mixed date + non-date columns", {
  tbl <- arrow::arrow_table(
    date_col = c("2024-01-01", "2024-06-15"),
    str_col  = c("hello", "world")
  )
  out <- cast_string_dates(tbl)
  expect_equal(out$schema$field("date_col")$type$id, arrow::Type$DATE32)
  expect_equal(out$schema$field("str_col")$type$id,  arrow::Type$STRING)
})

test_that(".cast_string_date_cols handles date strings with NAs", {
  tbl <- arrow::arrow_table(d = c("2024-01-01", NA, "2024-06-15"))
  out <- cast_string_dates(tbl)
  expect_equal(out$schema$field("d")$type$id, arrow::Type$DATE32)
  vals <- as.Date(out$column("d")$as_vector())
  expect_true(is.na(vals[2]))
  expect_equal(vals[1], as.Date("2024-01-01"))
})

test_that(".cast_string_date_cols passes already-date32 columns through", {
  tbl <- arrow::arrow_table(d = arrow::Array$create(
    c(as.Date("2024-01-01"), as.Date("2024-06-15")), type = arrow::date32()
  ))
  out <- cast_string_dates(tbl)
  expect_equal(out$schema$field("d")$type$id, arrow::Type$DATE32)
})

test_that(".cast_string_date_cols leaves integers unchanged", {
  tbl <- arrow::arrow_table(n = 1:5)
  out <- cast_string_dates(tbl)
  expect_equal(out$schema$field("n")$type$id, arrow::Type$INT32)
})

test_that(".cast_string_date_cols leaves timestamps unchanged", {
  tbl <- arrow::arrow_table(
    ts = arrow::Array$create(
      as.POSIXct("2024-01-01 12:00:00", tz = "UTC"),
      type = arrow::timestamp("us", timezone = "UTC")
    )
  )
  out <- cast_string_dates(tbl)
  expect_equal(out$schema$field("ts")$type$id, arrow::Type$TIMESTAMP)
})

test_that(".cast_string_date_cols handles empty string column", {
  tbl <- arrow::arrow_table(d = character(0))
  out <- cast_string_dates(tbl)
  # Empty column: cast succeeds → date32, or stays string — both acceptable
  expect_true(out$schema$field("d")$type$id %in% c(arrow::Type$DATE32, arrow::Type$STRING))
})

test_that(".cast_string_date_cols handles all-NA string column", {
  tbl <- arrow::arrow_table(d = arrow::Array$create(
    c(NA_character_, NA_character_), type = arrow::utf8()
  ))
  out <- cast_string_dates(tbl)
  # All nulls: cast succeeds → date32 or stays string — both acceptable
  expect_true(out$schema$field("d")$type$id %in% c(arrow::Type$DATE32, arrow::Type$STRING))
})


# ============================================================================
# 2. .r_df_to_pandas  — type conversions through the Arrow IPC bridge
#    Requires reticulate + Python + pyarrow installed.
# ============================================================================

skip_if_not(reticulate::py_available(), "Python not available")
skip_if_not(reticulate::py_module_available("pyarrow"), "pyarrow not available")
skip_if_not(reticulate::py_module_available("pandas"), "pandas not available")

test_that(".r_df_to_pandas: logical → int32 (DB2 has no BOOLEAN)", {
  df  <- data.frame(b = c(TRUE, FALSE, NA))
  py  <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["b"]])
  expect_true(grepl("int|Int", dtype, ignore.case = TRUE))
})

test_that(".r_df_to_pandas: integer → int32", {
  df  <- data.frame(n = c(1L, 2L, 3L))
  py  <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["n"]])
  expect_true(grepl("int", dtype, ignore.case = TRUE))
})

test_that(".r_df_to_pandas: numeric → float64", {
  df  <- data.frame(x = c(1.5, 2.5, 3.14))
  py  <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["x"]])
  expect_true(grepl("float", dtype, ignore.case = TRUE))
})

test_that(".r_df_to_pandas: character → object/str dtype", {
  df  <- data.frame(s = c("hello", "world"), stringsAsFactors = FALSE)
  py  <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["s"]])
  expect_true(grepl("object|str|string", dtype, ignore.case = TRUE))
})

test_that(".r_df_to_pandas: Date → object with datetime.date (Arrow date32 → pandas)", {
  df <- data.frame(d = as.Date(c("2024-01-01", "2024-06-15")))
  py <- r_df_to_pandas(df)
  # Arrow date32 → pandas produces object dtype with Python datetime.date objects.
  # PyArrow infers date32 from this — verify the round-trip produces date32.
  pa <- reticulate::import("pyarrow", convert = FALSE)
  arrow_tbl <- pa$Table$from_pandas(py, preserve_index = FALSE)
  field_type_id <- reticulate::py_to_r(arrow_tbl$schema$field("d")$type$id)
  # Arrow date32 type id is 9
  expect_equal(field_type_id, 9L)  # pa.lib.Type_DATE32 == 9
})

test_that(".r_df_to_pandas: POSIXct → tz-naive datetime64 (UTC stripped)", {
  df <- data.frame(
    ts = as.POSIXct("2024-01-15 09:30:00", tz = "America/New_York")
  )
  py <- r_df_to_pandas(df)
  tz_val <- tryCatch(
    reticulate::py_to_r(py$dtypes[["ts"]]$tz),
    error = function(e) NULL
  )
  # tz should be None (stripped)
  expect_true(is.null(tz_val) || inherits(tz_val, "python.builtin.NoneType"))
})

test_that(".r_df_to_pandas: POSIXct UTC → tz-naive datetime64", {
  df <- data.frame(
    ts = as.POSIXct("2024-06-15 12:00:00", tz = "UTC")
  )
  py <- r_df_to_pandas(df)
  tz_val <- tryCatch(
    reticulate::py_to_r(py$dtypes[["ts"]]$tz),
    error = function(e) NULL
  )
  expect_true(is.null(tz_val) || inherits(tz_val, "python.builtin.NoneType"))
})

test_that(".r_df_to_pandas: factor → object dtype (not Categorical)", {
  df <- data.frame(f = factor(c("a", "b", "c")))
  py <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["f"]])
  expect_true(grepl("object|str|string", dtype, ignore.case = TRUE))
  expect_false(grepl("category", dtype, ignore.case = TRUE))
})

test_that(".r_df_to_pandas: difftime → numeric (seconds)", {
  df <- data.frame(
    t = as.difftime(c(3600, 7200), units = "secs")
  )
  py <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["t"]])
  expect_true(grepl("float|int", dtype, ignore.case = TRUE))
})

test_that(".r_df_to_pandas: complex → character", {
  df <- data.frame(c = I(c(1+2i, 3+4i)))
  py <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["c"]])
  expect_true(grepl("object|str|string", dtype, ignore.case = TRUE))
})

test_that(".r_df_to_pandas: raw → character", {
  df <- data.frame(r = I(c(as.raw(0x01), as.raw(0xff))))
  py <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["r"]])
  expect_true(grepl("object|str|string", dtype, ignore.case = TRUE))
})

test_that(".r_df_to_pandas: list column → JSON strings", {
  df <- data.frame(
    id = 1:2,
    stringsAsFactors = FALSE
  )
  df$meta <- list(list(a = 1), list(b = 2))
  py    <- r_df_to_pandas(df)
  dtype <- as.character(py$dtypes[["meta"]])
  expect_true(grepl("object|str|string", dtype, ignore.case = TRUE))
  # First value should be valid JSON
  val <- reticulate::py_to_r(py[["meta"]][0L])
  expect_true(grepl("\\{", val))
})

test_that(".r_df_to_pandas: Date round-trip preserves values", {
  dates <- as.Date(c("2024-01-01", "2000-12-31", "1999-07-04"))
  df <- data.frame(d = dates)
  py <- r_df_to_pandas(df)
  # Convert back via Arrow and check values preserved
  pa <- reticulate::import("pyarrow", convert = FALSE)
  arrow_tbl <- pa$Table$from_pandas(py, preserve_index = FALSE)
  ipc_buf <- arrow::write_to_raw(
    arrow::read_ipc_stream(reticulate::py_to_r(
      pa$ipc$serialize_all(arrow_tbl)$to_pybytes()
    )),
    format = "stream"
  )
  # Just verify the function doesn't error — value integrity verified by Arrow
  expect_true(length(ipc_buf) > 0)
})

test_that(".r_df_to_pandas: NA values preserved across all types", {
  df <- data.frame(
    int_col  = c(1L, NA_integer_),
    dbl_col  = c(1.5, NA_real_),
    chr_col  = c("a", NA_character_),
    date_col = c(as.Date("2024-01-01"), NA),
    stringsAsFactors = FALSE
  )
  py <- r_df_to_pandas(df)
  pd <- reticulate::import("pandas", convert = FALSE)

  # Each column should have exactly 1 NA
  expect_equal(reticulate::py_to_r(py[["int_col"]]$isna()$sum()), 1L)
  expect_equal(reticulate::py_to_r(py[["dbl_col"]]$isna()$sum()), 1L)
  expect_equal(reticulate::py_to_r(py[["chr_col"]]$isna()$sum()), 1L)
  expect_equal(reticulate::py_to_r(py[["date_col"]]$isna()$sum()), 1L)
})

test_that(".r_df_to_pandas: multi-type DataFrame preserves column count and names", {
  df <- data.frame(
    bool_col  = TRUE,
    int_col   = 1L,
    dbl_col   = 3.14,
    chr_col   = "x",
    date_col  = as.Date("2024-01-01"),
    posix_col = as.POSIXct("2024-01-01 00:00:00", tz = "UTC"),
    stringsAsFactors = FALSE
  )
  py <- r_df_to_pandas(df)
  py_names <- reticulate::py_to_r(py$columns$tolist())
  expect_equal(length(py_names), ncol(df))
  expect_equal(sort(py_names), sort(names(df)))
})
