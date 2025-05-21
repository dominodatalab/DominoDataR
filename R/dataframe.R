#' Write a data frame to a table in the datasource
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to write to
#' @param table_name Name of the table to write to
#' @param data_frame Data frame containing the data to write
#' @param if_table_exists Action to take if the table already exists: 'fail' (default), 'replace', 'append', or 'truncate'
#' @param chunk_size Number of rows to insert in each batch for large data frames (default: 10000)
#' @param handle_mixed_types If TRUE (default), detect and handle mixed types in columns
#' @param force If TRUE, attempt to append data even if schema compatibility issues are detected (default: FALSE)
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
#' }
write_dataframe <- function(client, datasource, table_name, data_frame, 
                           if_table_exists = 'fail', chunk_size = 10000, 
                           handle_mixed_types = TRUE, force = FALSE, override = list()) {
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Convert R data frame to Python pandas DataFrame
  pandas <- reticulate::import("pandas")
  py_df <- pandas$DataFrame(data_frame)
  
  # Call the Python write_dataframe method
  ds_obj$write_dataframe(
    table_name = table_name,
    dataframe = py_df,
    if_table_exists = if_table_exists,
    chunksize = as.integer(chunk_size),
    handle_mixed_types = handle_mixed_types,
    force = force
  )
  
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
#'   all_users <- table_query(client, "my_datasource", "users")$all()
#'   active_users <- table_query(client, "my_datasource", "users")$
#'     filter("is_active = TRUE")$
#'     all()
#' }
table_query <- function(client, datasource, table_name, override = list()) {
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
      query_obj$select(columns)
      return(result)
    },
    
    # Method to filter results
    filter = function(condition) {
      query_obj$filter(condition)
      return(result)
    },
    
    # Method to order results
    order_by = function(order) {
      query_obj$order_by(order)
      return(result)
    },
    
    # Method to limit results
    limit = function(limit) {
      query_obj$limit(as.integer(limit))
      return(result)
    },
    
    # Method to set offset
    offset = function(offset) {
      query_obj$offset(as.integer(offset))
      return(result)
    },
    
    # Method to execute the query and return all results
    all = function() {
      py_result <- query_obj$all()
      return(reticulate::py_to_r(py_result))
    },
    
    # Method to get the first result
    first = function() {
      py_result <- query_obj$first()
      if (!reticulate::py_is_null_xptr(py_result)) {
        return(reticulate::py_to_r(py_result))
      } else {
        return(NULL)
      }
    },
    
    # Method to count results
    count = function() {
      return(as.integer(query_obj$count()))
    }
  )
  
  return(result)
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
#'   register_type(client, "my_datasource", "Date", "DATE")
#' }
register_type <- function(client, datasource, r_type, sql_type, override = list()) {
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
    stop(paste0("Unsupported R type: ", r_type))
  }
  
  # Register the type mapping
  ds_obj$register_type(py_type, sql_type)
  
  invisible(NULL)
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
#'   enable_sql_debug(client, "my_datasource", TRUE)
#' }
enable_sql_debug <- function(client, datasource, enabled = TRUE, override = list()) {
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Enable or disable SQL debugging
  ds_obj$enable_sql_debug(enabled)
  
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
#' }
get_type_mappings <- function(client, datasource, override = list()) {
  # Get the datasource object
  ds_obj <- client$get_datasource(datasource)
  
  # Add credentials
  credentials <- add_credentials(ds_obj$auth_type, override)
  
  # Get type mappings
  mappings <- ds_obj$get_type_mappings()
  
  # Convert to R list
  return(reticulate::py_to_r(mappings))
}
