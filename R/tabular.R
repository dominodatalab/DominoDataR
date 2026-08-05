# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#' Query a datasource and return an Arrow Table
#'
#' @import arrow
#'
#' This is the \strong{recommended function for running SELECT queries} against
#' any Domino datasource.  It works with all connector types, including the
#' native DB2 connector (\code{DB2NativeConfig}) and the legacy
#' Starburst/Trino connector (\code{DB2Config}).  Submit your SQL in the
#' dialect of the underlying database — no wrapper or translation is needed.
#'
#' @section Connector compatibility:
#' \describe{
#'   \item{DB2NativeConfig (native DB2)}{Use \code{query()} for all SELECT,
#'     VALUES, and WITH (CTE) statements.  For DDL and DML that returns no
#'     rows (INSERT, UPDATE, DELETE, MERGE, CREATE TABLE, etc.) use
#'     [execute_statement()] instead.  \strong{Do not use}
#'     [passthrough_query()] — it will error on this connector.}
#'   \item{DB2Config (legacy Starburst/Trino)}{Also supported.  For
#'     advanced DB2-native syntax that Trino cannot parse directly, use
#'     [passthrough_query()] which wraps the statement in
#'     \code{system.query()}.}
#' }
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to query
#' @param query The SQL SELECT (or VALUES / WITH) statement to run
#' @param override Configuration values to override ([add_override()])
#'
#' @return An [arrow::Table]
#'
#' @examples
#' \dontrun{
#' client <- datasource_client()
#'
#' # Basic query — result is an Arrow Table; convert to data.frame as needed
#' result <- query(client, "my_datasource", "SELECT * FROM my_table")
#' df <- as.data.frame(result)
#'
#' # Filter at the database (works on DB2NativeConfig and DB2Config alike)
#' recent <- as.data.frame(
#'   query(client, "my_datasource", "SELECT * FROM events WHERE event_date >= '2024-01-01'")
#' )
#'
#' # DB2-native syntax works directly on DB2NativeConfig — no passthrough needed
#' df <- as.data.frame(
#'   query(client, "my_db2_datasource",
#'         "SELECT DAYOFWEEK(hire_date), COUNT(*) FROM staff GROUP BY ROLLUP(DAYOFWEEK(hire_date))")
#' )
#'
#' # Use dplyr directly on the Arrow Table without pulling everything into R
#' library(dplyr)
#' summary <- result |> filter(status == "active") |> count(region) |> collect()
#' }
#'
#' @seealso [datasource_client()] to create the client, [table_query()] for a
#'   fluent query interface, [execute_statement()] for DDL/DML statements,
#'   [write_dataframe()] for bulk writes, [passthrough_query()] for legacy
#'   Starburst/Trino passthrough (DB2Config only)
#' @export
query <- function(client, datasource, query, override = list()) {
  datasource <- .cached_get_datasource(client, datasource)
  credentials <- DominoDataR::add_credentials(datasource$auth_type, override)
  result <- client$execute(
    datasource$identifier,
    query,
    reticulate::dict(override),
    reticulate::dict(credentials)
  )
  .cast_string_date_cols(result$reader$to_reader()$read_table())
}
