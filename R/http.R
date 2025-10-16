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

#' HTTP requests for Object Stores
#'
#' @details This is mostly for internal use by object store APIs.
#' @param verb A character string containing an HTTP verb, defaulting
#'   to \dQuote{GET}.
#' @param url Signed URL to download object at.
#' @param datasource_type Type of datasource the object is stored in.
#' @param config A list of config values for the REST call.
#' @param headers A list of request headers for the REST call.
#' @param request_body A character string containing request body data.
#' @param write_disk An argument like \code{\link[httr]{write_disk}} to
#'   write the result directly to disk.
#'
#' @export
#' @return a \code{\link[httr]{response}} object.
object_http <- function(verb = "GET",
                        url,
                        datasource_type,
                        config = list(),
                        headers = list(),
                        request_body = "",
                        write_disk = NULL) {
  if (datasource_type == "ADLSConfig") {
    headers["X-Ms-Blob-Type"] <- "BlockBlob"
  }
  if (datasource_type == "GenericS3Config") {
    config <- httr::config(ssl_verifypeer = FALSE)
  }
  if (datasource_type == "NetAppVolumeConfig") {
    token <- get_netapp_token()
    if (!is.null(token) && token != "") {
      headers["Authorization"] <- paste("Bearer", token)
    }
  }

  h <- do.call(httr::add_headers, headers)

  r <- make_request(verb, url, h, config, request_body, write_disk)
  if (r$status_code == 400) {
    new_url <- encode_url_path(url)
    r <- make_request(verb, new_url, h, config, request_body, write_disk)
  }
  r
}


#' Make the HTTP request
#'
#' @details Internal only to enable retry for malformed URLs.
#' @param verb A character string containing an HTTP verb
#' @param url Signed URL to download object at.
#' @param config A list of config values for the REST call.
#' @param headers A list of request headers for the REST call.
#' @param request_body A character string containing request body data.
#' @param write_disk An argument like \code{\link[httr]{write_disk}} to
#'   write the result directly to disk.
#'
#' @return a \code{\link[httr]{response}} object.
make_request <- function(verb, url, config, headers, request_body, write_disk) {
  if (verb == "GET") {
    if (!is.null(write_disk)) {
      r <- httr::GET(url, headers, config, write_disk)
    } else {
      r <- httr::GET(url, headers, config)
    }
  } else if (verb == "PUT") {
    if (is.character(request_body) && request_body == "") {
      r <- httr::PUT(url, headers, config)
    } else {
      r <- httr::PUT(url, headers, config, body = request_body)
    }
  }
  r
}


#' Properly escape a URL path
#'
#' @details Internal only to fix malformed URLs.
#' @param url URL to properly escape
#'
#' @return escaped url
encode_url_path <- function(url) {
  parsed <- urltools::url_parse(url)
  parsed$path <- urltools::url_encode(parsed$path)
  urltools::url_compose(parsed)
}


#' Get NetApp volume authentication token
#'
#' @details Retrieves JWT token for NetApp volume authentication.
#'   Checks DOMINO_TOKEN_FILE and DOMINO_API_PROXY environment variables.
#'
#' @return JWT token string or NULL if not available
#' @keywords internal
get_netapp_token <- function() {
  # Try to read from token file first
  token_file <- Sys.getenv("DOMINO_TOKEN_FILE", "")
  if (token_file != "" && file.exists(token_file)) {
    tryCatch({
      token <- readLines(token_file, n = 1, warn = FALSE)
      if (length(token) > 0 && token != "") {
        return(token)
      }
    }, error = function(e) {
      warning("Failed to read token from file: ", e$message)
    })
  }

  # Try to get token from API proxy
  api_proxy <- Sys.getenv("DOMINO_API_PROXY", "")
  if (api_proxy != "") {
    url <- paste0(api_proxy, "/access-token")
    tryCatch({
      response <- httr::GET(url)
      if (httr::status_code(response) == 200) {
        token <- httr::content(response, "text", encoding = "UTF-8")
        if (!is.null(token) && token != "") {
          return(token)
        }
      }
    }, error = function(e) {
      warning("Failed to retrieve token from API proxy: ", e$message)
    })
  }

  return(NULL)
}
