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
