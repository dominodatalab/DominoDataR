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

#' Query a datasource and returns an arrow Table
#'
#' @import arrow
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to query
#' @param query The query to run against the provided datasource
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
#' # Filter at the database
#' recent <- as.data.frame(
#'   query(client, "my_datasource", "SELECT * FROM events WHERE event_date >= '2024-01-01'")
#' )
#'
#' # Use dplyr directly on the Arrow Table without pulling everything into R
#' library(dplyr)
#' summary <- result |> filter(status == "active") |> count(region) |> collect()
#' }
#'
#' @seealso [datasource_client()] to create the client, [table_query()] for a fluent
#'   query interface, [execute_statement()] for DDL/DML, [write_dataframe()] for writes
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
