#' Write a data frame to a table in the datasource with automatic chunk size optimization
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to write to
#' @param table_name Name of the table to write to
#' @param data_frame Data frame containing the data to write
#' @param if_table_exists Action to take if the table already exists: 'fail' (default), 'replace', 'append', or 'truncate'
#' @param chunk_size Number of rows to insert in each batch. If NULL, auto-optimize based on data characteristics (default: NULL)
#' @param handle_mixed_types If TRUE (default), detect and handle mixed types in columns
#' @param force If TRUE, attempt to append data even if schema compatibility issues are detected (default: FALSE)
#' @param auto_optimize_chunks If TRUE (default), automatically calculate optimal chunk size
#' @param max_message_size_mb Maximum gRPC message size in MB for auto-optimization (default: 4.0)
#' @param override Configuration values to override ([add_override()])
#'
#' @return Invisible NULL
#' @export
#' @seealso \code{\link{query}} for executing raw SQL queries, \code{\link{table_query}} for a fluent query interface
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' df <- data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
#' 
#' # Auto-optimized chunking (default behavior)
#' write_dataframe(client, "my_datasource", "my_table", df)
#' 
#' # Manual chunk size (overrides auto-optimization)
#' write_dataframe(client, "my_datasource", "my_table", df, chunk_size = 5000)
#' 
#' # Auto-optimization with custom gRPC limit
#' write_dataframe(client, "my_datasource", "my_table", df, max_message_size_mb = 2.0)
#' }
write_dataframe <- function(client, datasource, table_name, data_frame,
                           if_table_exists = 'fail', chunk_size = NULL,
                           handle_mixed_types = TRUE, force = FALSE,
                           auto_optimize_chunks = TRUE, max_message_size_mb = 4.0,
                           override = list()) {
  # Input validation
  if (!is.data.frame(data_frame)) {
    stop("data_frame must be a data.frame object")
  }
  
  if (nrow(data_frame) == 0) {
    stop("data_frame cannot be empty")
  }
  
  if (!is.character(table_name) || length(table_name) != 1) {
    stop("table_name must be a single character string")
  }
  
  valid_options <- c('fail', 'replace', 'append', 'truncate')
  if (!if_table_exists %in% valid_options) {
    stop(paste("if_table_exists must be one of:", paste(valid_options, collapse = ", ")))
  }
  
  # Enhanced data conversion for R to Python compatibility
  data_frame <- .prepare_dataframe_for_python(data_frame)
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Convert R data frame to Python pandas DataFrame
  pandas <- reticulate::import("pandas")
  py_df <- pandas$DataFrame(data_frame)
  
  # Call the Python write_dataframe method with new parameters
  tryCatch({
    if (is.null(chunk_size)) {
      # Use auto-optimization
      ds_obj$write_dataframe(
        table_name = table_name,
        dataframe = py_df,
        if_table_exists = if_table_exists,
        chunksize = reticulate::py_none(),
        handle_mixed_types = handle_mixed_types,
        force = force,
        auto_optimize_chunks = auto_optimize_chunks,
        max_message_size_mb = max_message_size_mb
      )
    } else {
      # Use manual chunk size
      ds_obj$write_dataframe(
        table_name = table_name,
        dataframe = py_df,
        if_table_exists = if_table_exists,
        chunksize = as.integer(chunk_size),
        handle_mixed_types = handle_mixed_types,
        force = force,
        auto_optimize_chunks = auto_optimize_chunks,
        max_message_size_mb = max_message_size_mb
      )
    }
    
  }, error = function(e) {
    # Check for cleanup-related errors
    if (grepl("failed to clean up table", e$message, ignore.case = TRUE)) {
      warning("Partial data may exist due to failed write operation")
    }
    
    stop(paste("Failed to write dataframe to table:", e$message))
  })
  
  invisible(NULL)
}

#' Calculate optimal chunk size for a data frame
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param data_frame Data frame to analyze
#' @param max_message_size_mb Maximum message size in MB (default: 4.0)
#' @param safety_factor Safety multiplier to account for serialization overhead (default: 0.8)
#' @param override Configuration values to override ([add_override()])
#'
#' @return Integer indicating optimal chunk size in number of rows
#' @export
#' @seealso \code{\link{write_dataframe}} for writing data with optimized chunks
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' df <- data.frame(id = 1:10000, data = rep("large text", 10000))
#' optimal_chunk <- calculate_optimal_chunk_size(client, "my_datasource", df)
#' cat("Recommended chunk size:", optimal_chunk, "\n")
#' }
calculate_optimal_chunk_size <- function(client, datasource, data_frame, 
                                       max_message_size_mb = 4.0, safety_factor = 0.8,
                                       override = list()) {
  # Input validation
  if (!is.data.frame(data_frame)) {
    stop("data_frame must be a data.frame object")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Convert R data frame to Python pandas DataFrame
  pandas <- reticulate::import("pandas")
  py_df <- pandas$DataFrame(data_frame)
  
  # Call the Python calculate_optimal_chunk_size method
  tryCatch({
    result <- ds_obj$calculate_optimal_chunk_size(
      dataframe = py_df,
      max_message_size_mb = max_message_size_mb,
      safety_factor = safety_factor
    )
    return(as.integer(result))
  }, error = function(e) {
    stop(paste("Failed to calculate optimal chunk size:", e$message))
  })
}

#' Estimate message size for a given chunk size
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param data_frame Data frame to analyze
#' @param chunk_size Number of rows per chunk
#' @param override Configuration values to override ([add_override()])
#'
#' @return Numeric value indicating estimated message size in MB
#' @export
#' @seealso \code{\link{calculate_optimal_chunk_size}} for calculating optimal chunks
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' df <- data.frame(id = 1:10000, data = rep("large text", 10000))
#' estimated_size <- estimate_message_size(client, "my_datasource", df, 5000)
#' cat("Estimated message size for 5000 rows:", estimated_size, "MB\n")
#' }
estimate_message_size <- function(client, datasource, data_frame, chunk_size, override = list()) {
  # Input validation
  if (!is.data.frame(data_frame)) {
    stop("data_frame must be a data.frame object")
  }
  
  if (!is.numeric(chunk_size) || length(chunk_size) != 1 || chunk_size < 1) {
    stop("chunk_size must be a positive integer")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Convert R data frame to Python pandas DataFrame
  pandas <- reticulate::import("pandas")
  py_df <- pandas$DataFrame(data_frame)
  
  # Call the Python estimate_message_size method
  tryCatch({
    result <- ds_obj$estimate_message_size(
      dataframe = py_df,
      chunk_size = as.integer(chunk_size)
    )
    return(as.numeric(result))
  }, error = function(e) {
    stop(paste("Failed to estimate message size:", e$message))
  })
}

#' Set gRPC message limits for the datasource
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param max_message_size_mb Maximum message size in MB (default: 64.0)
#' @param override Configuration values to override ([add_override()])
#'
#' @return Invisible NULL
#' @export
#' @seealso \code{\link{calculate_optimal_chunk_size}} for chunk size optimization
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' set_grpc_message_limits(client, "my_datasource", max_message_size_mb = 64.0)
#' }
set_grpc_message_limits <- function(client, datasource, max_message_size_mb = 64.0, override = list()) {
  # Input validation
  if (!is.numeric(max_message_size_mb) || length(max_message_size_mb) != 1 || max_message_size_mb <= 0) {
    stop("max_message_size_mb must be a positive number")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Call the Python set_grpc_message_limits method
  tryCatch({
    ds_obj$set_grpc_message_limits(max_message_size_mb = max_message_size_mb)
    cat("gRPC message limits updated to", max_message_size_mb, "MB\n")
  }, error = function(e) {
    stop(paste("Failed to set gRPC message limits:", e$message))
  })
  
  invisible(NULL)
}

#' Check if a table exists in the database
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param table_name Name of the table to check
#' @param override Configuration values to override ([add_override()])
#'
#' @return Logical value indicating if table exists
#' @export
#' @seealso \code{\link{write_dataframe}} for writing data to tables
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' if (table_exists(client, "my_datasource", "my_table")) {
#'   cat("Table exists\n")
#' }
#' }
table_exists <- function(client, datasource, table_name, override = list()) {
  # Input validation
  if (!is.character(table_name) || length(table_name) != 1) {
    stop("table_name must be a single character string")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Check if table exists
  tryCatch({
    result <- ds_obj$table_exists(table_name)
    return(as.logical(result))
  }, error = function(e) {
    stop(paste("Failed to check table existence:", e$message))
  })
}

#' Get database type with enhanced detection
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param override Configuration values to override ([add_override()])
#'
#' @return Character string indicating database type
#' @export
#' @seealso \code{\link{set_db_type_override}} for manually setting database type
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' db_type <- get_db_type(client, "my_datasource")
#' cat("Database type:", db_type, "\n")
#' }
get_db_type <- function(client, datasource, override = list()) {
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Get database type
  tryCatch({
    db_type <- ds_obj$get_db_type()
    return(as.character(db_type))
  }, error = function(e) {
    stop(paste("Failed to get database type:", e$message))
  })
}

#' Set database type override
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param db_type Database type to force (e.g., 'db2', 'postgresql', 'mysql', 'oracle', 'sqlserver') or NULL to remove override
#' @param override Configuration values to override ([add_override()])
#'
#' @return Invisible NULL
#' @export
#' @seealso \code{\link{get_db_type}} for getting current database type, \code{\link{get_supported_db_types}} for supported types
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' 
#' # Force DB2 detection
#' set_db_type_override(client, "my_datasource", "db2")
#' 
#' # Remove override
#' set_db_type_override(client, "my_datasource", NULL)
#' }
set_db_type_override <- function(client, datasource, db_type, override = list()) {
  # Input validation
  if (!is.null(db_type) && (!is.character(db_type) || length(db_type) != 1)) {
    stop("db_type must be a single character string or NULL")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Set database type override
  tryCatch({
    if (is.null(db_type)) {
      ds_obj$set_db_type_override(reticulate::py_none())
      cat("Database type override removed - auto-detection re-enabled\n")
    } else {
      ds_obj$set_db_type_override(db_type)
      cat("Database type override set to:", db_type, "\n")
    }
  }, error = function(e) {
    stop(paste("Failed to set database type override:", e$message))
  })
  
  invisible(NULL)
}

#' Get current database type override
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param override Configuration values to override ([add_override()])
#'
#' @return Character string of current override or NULL if auto-detection is enabled
#' @export
#' @seealso \code{\link{set_db_type_override}} for setting database type override
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' current_override <- get_db_type_override(client, "my_datasource")
#' if (!is.null(current_override)) {
#'   cat("Database type manually set to:", current_override, "\n")
#' } else {
#'   cat("Using auto-detection\n")
#' }
#' }
get_db_type_override <- function(client, datasource, override = list()) {
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Get database type override
  tryCatch({
    result <- ds_obj$get_db_type_override()
    if (reticulate::py_is_null_xptr(result)) {
      return(NULL)
    } else {
      return(as.character(result))
    }
  }, error = function(e) {
    stop(paste("Failed to get database type override:", e$message))
  })
}

#' Get supported database types
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param override Configuration values to override ([add_override()])
#'
#' @return Character vector of supported database types
#' @export
#' @seealso \code{\link{set_db_type_override}} for setting database type override
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' supported <- get_supported_db_types(client, "my_datasource")
#' cat("Supported database types:", paste(supported, collapse = ", "), "\n")
#' }
get_supported_db_types <- function(client, datasource, override = list()) {
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Get supported database types
  tryCatch({
    result <- ds_obj$get_supported_db_types()
    return(as.character(reticulate::py_to_r(result)))
  }, error = function(e) {
    stop(paste("Failed to get supported database types:", e$message))
  })
}

#' Drop a table quietly (suppress errors)
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param table_name Name of the table to drop
#' @param override Configuration values to override ([add_override()])
#'
#' @return Invisible NULL
#' @export
#' @seealso \code{\link{write_dataframe}} for writing data to tables
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' drop_table_quietly(client, "my_datasource", "test_table")
#' }
drop_table_quietly <- function(client, datasource, table_name, override = list()) {
  # Input validation
  if (!is.character(table_name) || length(table_name) != 1) {
    stop("table_name must be a single character string")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Call the Python _drop_table_quietly method
  tryCatch({
    ds_obj$`_drop_table_quietly`(table_name)
    message("Table dropped quietly: ", table_name)
  }, error = function(e) {
    warning("Failed to drop table quietly: ", e$message)
  })
  
  invisible(NULL)
}

#' Create a fluent query interface for a table
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to query
#' @param table_name Name of the table to query
#' @param override Configuration values to override ([add_override()])
#'
#' @return A list with methods for building and executing queries
#' @export
#' @seealso \code{\link{query}} for executing raw SQL queries, \code{\link{write_dataframe}} for writing data to tables
#' @examples
#' \dontrun{
#' client <- datasource_client()
#'
#' # Get all records
#' all_records <- table_query(client, "my_datasource", "my_table")$all()
#'
#' # Chained operations
#' active_records <- table_query(client, "my_datasource", "my_table")$
#'   filter("is_active = TRUE")$
#'   select("name, age")$
#'   order_by("age DESC")$
#'   all()
#'
#' # Get first result
#' first_record <- table_query(client, "my_datasource", "my_table")$first()
#'
#' # Count results
#' record_count <- table_query(client, "my_datasource", "my_table")$count()
#' }
table_query <- function(client, datasource, table_name, override = list()) {
  # Input validation
  if (!is.character(table_name) || length(table_name) != 1) {
    stop("table_name must be a single character string")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Create the TableQuery object
  query_obj <- ds_obj$table(table_name)
  
  # Create an R wrapper for the Python TableQuery object
  result <- list(
    # Method to select columns
    select = function(columns) {
      if (!is.character(columns) || length(columns) != 1) {
        stop("columns must be a single character string (comma-separated column names)")
      }
      
      query_obj$select(columns)
      return(result)
    },
    
    # Method to filter results
    filter = function(condition) {
      if (!is.character(condition) || length(condition) != 1) {
        stop("condition must be a single character string containing SQL WHERE condition")
      }
      
      query_obj$filter(condition)
      return(result)
    },
    
    # Method to order results
    order_by = function(order) {
      if (!is.character(order) || length(order) != 1) {
        stop("order must be a single character string containing SQL ORDER BY expression")
      }
      
      query_obj$order_by(order)
      return(result)
    },
    
    # Method to limit results
    limit = function(limit) {
      if (!is.numeric(limit) || length(limit) != 1 || limit < 1) {
        stop("limit must be a positive integer")
      }
      
      query_obj$limit(as.integer(limit))
      return(result)
    },
    
    # Method to set offset
    offset = function(offset) {
      if (!is.numeric(offset) || length(offset) != 1 || offset < 0) {
        stop("offset must be a non-negative integer")
      }
      
      query_obj$offset(as.integer(offset))
      return(result)
    },
    
    # Method to execute the query and return all results
    all = function() {
      tryCatch({
        py_result <- query_obj$all()
        return(.convert_pandas_to_r(py_result))
      }, error = function(e) {
        stop(paste("Failed to execute query:", e$message))
      })
    },
    
    # Method to get the first result
    first = function() {
      tryCatch({
        py_result <- query_obj$first()
        if (!reticulate::py_is_null_xptr(py_result)) {
          return(.convert_pandas_series_to_r(py_result))
        } else {
          return(NULL)
        }
      }, error = function(e) {
        stop(paste("Failed to get first result:", e$message))
      })
    },
    
    # Method to count results
    count = function() {
      tryCatch({
        return(as.integer(query_obj$count()))
      }, error = function(e) {
        stop(paste("Failed to count results:", e$message))
      })
    }
  )
  
  return(result)
}

#' Wrap a query for database passthrough
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param query SQL query to wrap
#' @param override Configuration values to override ([add_override()])
#'
#' @return Wrapped query string
#' @export
#' @seealso \code{\link{passthrough_query}} for executing wrapped queries directly, \code{\link{query}} for standard queries
#' @examples
#' \dontrun{
#' client <- datasource_client()
#'
#' # Wrap a complex query for inspection
#' complex_query <- "SELECT * FROM my_table u JOIN orders o ON u.id = o.user_id ORDER BY u.created_date"
#' wrapped <- wrap_passthrough_query(client, "my_datasource", complex_query)
#' print(wrapped)
#'
#' # Then execute manually
#' result <- query(client, "my_datasource", wrapped)
#' }
wrap_passthrough_query <- function(client, datasource, query, override = list()) {
  # Input validation
  if (!is.character(query) || length(query) != 1) {
    stop("query must be a single character string")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Wrap the query
  tryCatch({
    wrapped <- ds_obj$wrap_passthrough_query(query)
    return(as.character(wrapped))
  }, error = function(e) {
    stop(paste("Failed to wrap query:", e$message))
  })
}

#' Execute a query with database passthrough wrapper
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param query SQL query to execute with passthrough
#' @param override Configuration values to override ([add_override()])
#'
#' @return Query result as data.frame
#' @export
#' @seealso \code{\link{wrap_passthrough_query}} for getting wrapped query strings, \code{\link{query}} for standard queries, \code{\link{table_query}} for fluent queries
#' @examples
#' \dontrun{
#' client <- datasource_client()
#'
#' # Execute complex query with passthrough
#' complex_query <- "SELECT * FROM my_table u JOIN orders o ON u.id = o.user_id ORDER BY u.created_date"
#' result <- passthrough_query(client, "my_datasource", complex_query)
#'
#' # Use data source-specific functions
#' db_specific <- "SELECT user_id, REGEXP_EXTRACT(email, '@(.*)') as domain FROM my_table"
#' result <- passthrough_query(client, "my_datasource", db_specific)
#' }
passthrough_query <- function(client, datasource, query, override = list()) {
  # Input validation
  if (!is.character(query) || length(query) != 1) {
    stop("query must be a single character string")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Execute passthrough query
  tryCatch({
    result <- ds_obj$passthrough_query(query)
    py_df <- result$to_pandas()
    return(.convert_pandas_to_r(py_df))
  }, error = function(e) {
    stop(paste("Failed to execute passthrough query:", e$message))
  })
}

#' Register a custom type mapping
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param r_type R type (e.g., "Date", "POSIXct", "integer")
#' @param sql_type SQL type to map to (e.g., "DATE", "TIMESTAMP", "INTEGER")
#' @param override Configuration values to override ([add_override()])
#'
#' @return Invisible NULL
#' @export
#' @seealso \code{\link{get_type_mappings}} for viewing current type mappings, \code{\link{write_dataframe}} for writing data with custom types
#' @examples
#' \dontrun{
#' client <- datasource_client()
#'
#' # Register a Date type mapping
#' register_type(client, "my_datasource", "Date", "DATE")
#'
#' # Register a custom numeric precision
#' register_type(client, "my_datasource", "numeric", "NUMERIC(10,2)")
#' }
register_type <- function(client, datasource, r_type, sql_type, override = list()) {
  # Input validation
  if (!is.character(r_type) || length(r_type) != 1) {
    stop("r_type must be a single character string")
  }
  
  if (!is.character(sql_type) || length(sql_type) != 1) {
    stop("sql_type must be a single character string")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Enhanced R type to Python type mapping
  py_types <- .get_python_type_mappings()
  
  # Get Python type equivalent
  py_type <- py_types[[r_type]]
  if (is.null(py_type)) {
    available_types <- paste(names(py_types), collapse = ", ")
    stop(paste0("Unsupported R type: '", r_type, "'. Available types: ", available_types))
  }
  
  # Register the type mapping
  tryCatch({
    ds_obj$register_type(py_type, sql_type)
    cat("Successfully registered type mapping:", r_type, "->", sql_type, "\n")
  }, error = function(e) {
    stop(paste("Failed to register type mapping:", e$message))
  })
  
  invisible(NULL)
}

#' Get type mappings
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param override Configuration values to override ([add_override()])
#'
#' @return A named list of type mappings
#' @export
#' @seealso \code{\link{register_type}} for adding custom type mappings, \code{\link{write_dataframe}} for writing data with custom types
#' @examples
#' \dontrun{
#' client <- datasource_client()
#' mappings <- get_type_mappings(client, "my_datasource")
#' print(mappings)
#'
#' # View specific type mapping
#' if ("str" %in% names(mappings)) {
#'   cat("String type maps to:", mappings[["str"]], "\n")
#' }
#' }
get_type_mappings <- function(client, datasource, override = list()) {
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Get type mappings
  tryCatch({
    mappings <- ds_obj$get_type_mappings()
    # Convert to R list
    result <- reticulate::py_to_r(mappings)
    # Ensure it's a named list
    if (!is.list(result)) {
      result <- as.list(result)
    }
    
    return(result)
  }, error = function(e) {
    stop(paste("Failed to get type mappings:", e$message))
  })
}

#' Enable SQL debugging
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource
#' @param enabled If TRUE (default), enable SQL debugging; if FALSE, disable it
#' @param override Configuration values to override ([add_override()])
#'
#' @return Invisible NULL
#' @export
#' @seealso \code{\link{query}} for executing raw SQL queries, \code{\link{table_query}} for a fluent query interface
#' @examples
#' \dontrun{
#' client <- datasource_client()
#'
#' # Enable SQL debugging
#' enable_sql_debug(client, "my_datasource", TRUE)
#'
#' # Now SQL statements will be logged
#' result <- table_query(client, "my_datasource", "my_table")$
#'   filter("age > 30")$
#'   all()
#'
#' # Disable SQL debugging
#' enable_sql_debug(client, "my_datasource", FALSE)
#' }
enable_sql_debug <- function(client, datasource, enabled = TRUE, override = list()) {
  # Input validation
  if (!is.logical(enabled) || length(enabled) != 1) {
    stop("enabled must be TRUE or FALSE")
  }
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Enable or disable SQL debugging
  tryCatch({
    ds_obj$enable_sql_debug(enabled)
    
    if (enabled) {
      # Configure Python logging to be visible in R
      logging <- reticulate::import("logging")
      
      # Set logging level to DEBUG for the domino_data logger
      domino_logger <- logging$getLogger("domino_data.data_sources")
      domino_logger$setLevel(logging$DEBUG)
      
      # Ensure we have a handler that outputs to stdout (visible in R)
      if (length(domino_logger$handlers) == 0) {
        handler <- logging$StreamHandler()
        formatter <- logging$Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler$setFormatter(formatter)
        domino_logger$addHandler(handler)
      }
      
      # Set the root logger level as well
      logging$getLogger()$setLevel(logging$DEBUG)
      cat("SQL debugging enabled for R session\n")
    } else {
      cat("SQL debugging disabled\n")
    }
    
  }, error = function(e) {
    stop(paste("Failed to configure SQL debugging:", e$message))
  })
  
  invisible(NULL)
}

# =============================================================================
# HELPER FUNCTIONS (Internal)
# =============================================================================

#' Prepare R data.frame for Python conversion
#' @param data_frame R data.frame to prepare
#' @return Prepared data.frame
#' @keywords internal
.prepare_dataframe_for_python <- function(data_frame) {
  # Enhanced data conversion for R to Python compatibility
  result <- as.data.frame(lapply(data_frame, function(col) {
    # Convert factors to characters
    if (is.factor(col)) {
      return(as.character(col))
    }
    
    # Convert difftime to numeric
    if (inherits(col, "difftime")) {
      return(as.numeric(col))
    }
    
    # Convert Date to character (will be parsed by pandas)
    if (inherits(col, "Date")) {
      return(as.character(col))
    }
    
    # Convert POSIXct/POSIXlt to character with proper format
    if (inherits(col, c("POSIXct", "POSIXlt"))) {
      return(format(col, "%Y-%m-%d %H:%M:%S"))
    }
    
    # Convert complex to character
    if (is.complex(col)) {
      return(as.character(col))
    }
    
    # Convert raw to character
    if (is.raw(col)) {
      return(as.character(col))
    }
    
    # Handle list columns (convert to JSON strings)
    if (is.list(col) && !is.data.frame(col)) {
      return(sapply(col, function(x) {
        if (is.null(x)) return(NA_character_)
        tryCatch(jsonlite::toJSON(x, auto_unbox = TRUE), 
                error = function(e) as.character(x))
      }))
    }
    
    col
  }), stringsAsFactors = FALSE)
  
  return(result)
}

#' Convert pandas DataFrame to R data.frame with proper type handling
#' @param py_df Python pandas DataFrame
#' @return R data.frame
#' @keywords internal
.convert_pandas_to_r <- function(py_df) {
  # Convert pandas DataFrame to R with enhanced type handling
  r_df <- reticulate::py_to_r(py_df)
  
  # Post-process columns for better R compatibility
  for (col_name in names(r_df)) {
    col <- r_df[[col_name]]
    
    # Try to parse datetime strings back to POSIXct
    if (is.character(col) && length(col) > 0) {
      # Check if it looks like a datetime
      sample_val <- col[!is.na(col)][1]
      if (!is.na(sample_val) && grepl("^\\d{4}-\\d{2}-\\d{2}", sample_val)) {
        parsed_date <- tryCatch({
          as.POSIXct(col, format = "%Y-%m-%d %H:%M:%S")
        }, error = function(e) NULL)
        
        if (!is.null(parsed_date) && sum(!is.na(parsed_date)) > 0) {
          r_df[[col_name]] <- parsed_date
        }
      }
    }
  }
  
  return(r_df)
}

#' Convert pandas Series to R vector with proper type handling
#' @param py_series Python pandas Series
#' @return R vector
#' @keywords internal
.convert_pandas_series_to_r <- function(py_series) {
  # Convert pandas Series to R vector
  r_vector <- reticulate::py_to_r(py_series)
  
  # If it's a data.frame with one row, convert to named vector
  if (is.data.frame(r_vector) && nrow(r_vector) == 1) {
    result <- as.list(r_vector[1, ])
    names(result) <- names(r_vector)
    return(result)
  }
  
  return(r_vector)
}

#' Get enhanced Python type mappings for R types
#' @return Named list of Python types
#' @keywords internal
.get_python_type_mappings <- function() {
  # Enhanced R type to Python type mapping
  list(
    "logical" = reticulate::import_builtins()$bool,
    "integer" = reticulate::import_builtins()$int,
    "numeric" = reticulate::import_builtins()$float,
    "double" = reticulate::import_builtins()$float,
    "character" = reticulate::import_builtins()$str,
    "factor" = reticulate::import_builtins()$str,
    "Date" = reticulate::import("datetime")$date,
    "POSIXct" = reticulate::import("datetime")$datetime,
    "POSIXlt" = reticulate::import("datetime")$datetime,
    "list" = reticulate::import_builtins()$list,
    "data.frame" = reticulate::import_builtins()$dict,
    "matrix" = reticulate::import_builtins()$list,
    "array" = reticulate::import_builtins()$list,
    "complex" = reticulate::import_builtins()$str,
    "raw" = reticulate::import_builtins()$bytes,
    "difftime" = reticulate::import_builtins()$float
  )
}
