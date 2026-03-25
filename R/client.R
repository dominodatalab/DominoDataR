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

#' Create a client to Domino datasources
#'
#' @param api_key string key to override the environment variable
#' @param token_file location of file to read token from
#' @param token_url url of the location to read the token from
#' @param token token to be used to authenticate
#'
#' @return A `domino_data.data_sources.DataSourceClient`.
#'
#' @examples
#' \dontrun{
#' # Default — reads credentials from the Domino environment automatically
#' client <- datasource_client()
#'
#' # Run a query and get the result as a data.frame
#' df <- as.data.frame(query(client, "my_datasource", "SELECT * FROM my_table"))
#'
#' # Write a data.frame back to a table
#' write_dataframe(client, "my_datasource", "my_table", df)
#'
#' # Execute DDL / DML that returns no result set
#' execute_statement(client, "my_datasource", "TRUNCATE TABLE my_table IMMEDIATE")
#' }
#'
#' @seealso [query()], [write_dataframe()], [execute_statement()]
#' @export
datasource_client <- function(api_key = NULL, token_file = NULL, token_url = NULL, token =  NULL) {
  envvar <- c("DOMINO_CLIENT_SOURCE" = "R")
  if (!is.null(api_key) || !is.null(token_file) || !is.null(token_url) || !is.null(token)) {
    client <- withr::with_envvar(
      new = envvar,
      domino_data_sources$DataSourceClient(api_key, token_file, token_url, token)
    )
  } else {
    client <- withr::with_envvar(
      new = envvar,
      domino_data_sources$DataSourceClient()
    )
  }
  return(client)
}


#' Cached wrapper around client$get_datasource()
#'
#' @details Internal. Calls `client$get_datasource(name)` once per datasource
#'   name per session and returns the cached result on subsequent calls,
#'   avoiding repeated 10–15 s HTTP round-trips.
#' @param client A datasource client as returned by [datasource_client()].
#' @param datasource Datasource name (character) or already-resolved object.
#' @return The resolved datasource object.
#' @keywords internal
.cached_get_datasource <- function(client, datasource) {
  if (!is.character(datasource)) return(datasource)
  # Prefix with the Python object id so two clients that share a datasource
  # name (e.g. pointing to different Domino environments) never collide.
  cache_key <- paste0(reticulate::py_id(client), ":", datasource)
  if (!exists(cache_key, envir = .ds_cache, inherits = FALSE)) {
    assign(cache_key, client$get_datasource(datasource), envir = .ds_cache)
  }
  .ds_cache[[cache_key]]
}


#' Create a client for NetApp volumes
#'
#' @param token_file location of file to read token from
#' @param token_url url of the location to read the token from
#' @param token token to be used to authenticate
#'
#' @return A `domino_data.netapp_volumes.NetAppVolumeClient`.
#' @export
netapp_volume_client <- function(token_file = NULL, token_url = NULL, token = NULL) {
  envvar <- c("DOMINO_CLIENT_SOURCE" = "R")
  if (!is.null(token_file) || !is.null(token_url) || !is.null(token)) {
    client <- withr::with_envvar(
      new = envvar,
      netapp_volumes$NetAppVolumeClient(token_file, token_url, token)
    )
  } else {
    client <- withr::with_envvar(
      new = envvar,
      netapp_volumes$NetAppVolumeClient()
    )
  }
  return(client)
}
