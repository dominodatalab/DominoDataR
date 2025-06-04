#' Write a data frame to a table in the datasource
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to write to
#' @param table_name Name of the table to write to
#' @param data_frame Data frame containing the data to write
#' @param if_table_exists Action to take if the table already exists: 'fail' (default), 'replace', 'append', or 'truncate'
#' @param chunk_size Number of rows to insert in each batch for large data frames (default: 20000)
#' @param handle_mixed_types If TRUE (default), detect and handle mixed types in columns
#' @param force If TRUE, attempt to append data even if schema compatibility issues are detected (default: FALSE)
#' @param verbose If TRUE, print progress messages (default: FALSE)
#' @param override Configuration values to override ([add_override()])
#'
#' @return Invisible NULL
#' @export
#' @seealso \code{\link{query}} for executing raw SQL queries, \code{\link{table_query}} for a fluent query interface
#' @examples
#' \dontrun{
#'   client <- datasource_client()
#'   df <- data.frame(id = 1:3, name = c("Alice", "Bob", "Charlie"))
#'   write_dataframe(client, "my_datasource", "users", df, if_table_exists = "replace")
#'   
#'   # With verbose output
#'   write_dataframe(client, "my_datasource", "users", df, verbose = TRUE)
#' }
write_dataframe <- function(client, datasource, table_name, data_frame, 
                           if_table_exists = 'fail', chunk_size = 20000, 
                           handle_mixed_types = TRUE, force = FALSE, 
                           verbose = FALSE, override = list()) {
  
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
  
  if (verbose) {
    cat("Writing", nrow(data_frame), "rows to table:", table_name, "\n")
    cat("Mode:", if_table_exists, "\n")
  }
  
  # Convert factors to characters and difftime to numeric
  data_frame <- as.data.frame(lapply(data_frame, function(col) {
    if (is.factor(col)) return(as.character(col))
    if (inherits(col, "difftime")) return(as.numeric(col))
    col
  }), stringsAsFactors = FALSE)
  
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Convert R data frame to Python pandas DataFrame
  pandas <- reticulate::import("pandas")
  py_df <- pandas$DataFrame(data_frame)
  
  if (verbose) {
    cat("Converted R data.frame to pandas DataFrame\n")
    cat("Calling Python write_dataframe method...\n")
  }
  
  # Call the Python write_dataframe method
  tryCatch({
    if (verbose && nrow(data_frame) > chunk_size) {
      pb <- utils::txtProgressBar(min = 0, max = ceiling(nrow(data_frame)/chunk_size), style = 3)
    }
    
    ds_obj$write_dataframe(
      table_name = table_name,
      dataframe = py_df,
      if_table_exists = if_table_exists,
      chunksize = as.integer(chunk_size),
      handle_mixed_types = handle_mixed_types,
      force = force
    )
    
    if (verbose && nrow(data_frame) > chunk_size) {
      utils::setTxtProgressBar(pb, ceiling(nrow(data_frame)/chunk_size))
      close(pb)
    }
    
    if (verbose) {
      cat("Write operation completed successfully\n")
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
#'   client <- datasource_client()
#'   drop_table_quietly(client, "my_datasource", "test_table")
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
#'   client <- datasource_client()
#'   
#'   # Get all users
#'   all_users <- table_query(client, "my_datasource", "users")$all()
#'   
#'   # Chained operations
#'   active_users <- table_query(client, "my_datasource", "users")$
#'     filter("is_active = TRUE")$
#'     select("name, age")$
#'     order_by("age DESC")$
#'     all()
#'     
#'   # Get first result
#'   first_user <- table_query(client, "my_datasource", "users")$first()
#'   
#'   # Count results
#'   user_count <- table_query(client, "my_datasource", "users")$count()
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
        return(reticulate::py_to_r(py_result))
      }, error = function(e) {
        stop(paste("Failed to execute query:", e$message))
      })
    },
    
    # Method to get the first result
    first = function() {
      tryCatch({
        py_result <- query_obj$first()
        if (!reticulate::py_is_null_xptr(py_result)) {
          return(reticulate::py_to_r(py_result))
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
#'   client <- datasource_client()
#'   
#'   # Wrap a complex query for inspection
#'   complex_query <- "SELECT * FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.created_date"
#'   wrapped <- wrap_passthrough_query(client, "<data_source>", complex_query)
#'   print(wrapped)
#'   
#'   # Then execute manually
#'   result <- query(client, "<data_source>", wrapped)
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
    return(wrapped)
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
#' @return Query result
#' @export
#' @seealso \code{\link{wrap_passthrough_query}} for getting wrapped query strings, \code{\link{query}} for standard queries, \code{\link{table_query}} for fluent queries
#' @examples
#' \dontrun{
#'   client <- datasource_client()
#'   
#'   # Execute complex query with passthrough
#'   complex_query <- "SELECT * FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.created_date"
#'   result <- passthrough_query(client, "<data_source>", complex_query)
#'   
#'   # Use data source-specific functions
#'   db_specific <- "SELECT user_id, REGEXP_EXTRACT(email, '@(.*)') as domain FROM users"
#'   result <- passthrough_query(client, "<data_source>", db_specific)
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
    return(reticulate::py_to_r(result))
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
#'   client <- datasource_client()
#'   
#'   # Register a Date type mapping
#'   register_type(client, "my_datasource", "Date", "DATE")
#'   
#'   # Register a custom numeric precision
#'   register_type(client, "my_datasource", "numeric", "NUMERIC(10,2)")
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
  
  # Map R type name to Python type
  py_types <- list(
    "logical" = reticulate::import_builtins()$bool,
    "integer" = reticulate::import_builtins()$int,
    "numeric" = reticulate::import_builtins()$float,
    "double" = reticulate::import_builtins()$float,
    "character" = reticulate::import_builtins()$str,
    "Date" = reticulate::import("datetime")$date,
    "POSIXct" = reticulate::import("datetime")$datetime,
    "POSIXlt" = reticulate::import("datetime")$datetime,
    "list" = reticulate::import_builtins()$list,
    "data.frame" = reticulate::import_builtins()$dict
  )
  
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
#'   client <- datasource_client()
#'   mappings <- get_type_mappings(client, "my_datasource")
#'   print(mappings)
#'   
#'   # View specific type mapping
#'   if ("str" %in% names(mappings)) {
#'     cat("String type maps to:", mappings[["str"]], "\n")
#'   }
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
#'   client <- datasource_client()
#'   
#'   # Enable SQL debugging
#'   enable_sql_debug(client, "my_datasource", TRUE)
#'   
#'   # Now SQL statements will be logged
#'   result <- table_query(client, "my_datasource", "users")$
#'     filter("age > 30")$
#'     all()
#'   
#'   # Disable SQL debugging
#'   enable_sql_debug(client, "my_datasource", FALSE)
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
