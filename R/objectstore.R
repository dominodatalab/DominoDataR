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

#' List the keys starting with prefix in an object store
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to query
#' @param prefix Prefix to filter keys to list
#' @param override Configuration values to override ([add_override()])
#' @param page_size Number of objects to fetch
#'
#' @return A vector or string keys
#' @export
list_keys <- function(client,
                      datasource,
                      prefix = "",
                      override = list(),
                      page_size = 50) {
  datasource <- client$get_datasource(datasource)
  credentials <- DominoDataR::add_credentials(datasource$auth_type, override)
  client$list_keys(
    datasource$identifier,
    prefix,
    as.integer(page_size),
    reticulate::dict(override),
    reticulate::dict(credentials)
  )
}

#' Retrieve an object from a datasource
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to query
#' @param object The object to retrieve
#' @param as Passed through to \code{\link[httr]{content}}
#' @param override Configuration values to override ([add_override()])
#'
#' @return Raw vector representation of the object
#' @export
get_object <- function(client,
                       datasource,
                       object,
                       as = "raw",
                       override = list()) {
  datasource <- client$get_datasource(datasource)
  credentials <- DominoDataR::add_credentials(datasource$auth_type, override)
  url <- client$get_key_url(
    datasource$identifier,
    object,
    FALSE,
    reticulate::dict(override),
    reticulate::dict(credentials)
  )
  r <- DominoDataR::object_http(
    "GET",
    url,
    datasource_type = datasource$datasource_type
  )
  httr::content(r, as = as)
}

#' Save an object from a datasource to a local file
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to query
#' @param object The object to retrieve
#' @param file File path to save object at. Defaults to the object base name.
#' @param override Configuration values to override ([add_override()])
#'
#' @return File path where the object was saved
#' @export
save_object <- function(client,
                        datasource,
                        object,
                        file = basename(object),
                        override = list()) {
  datasource <- client$get_datasource(datasource)
  credentials <- DominoDataR::add_credentials(datasource$auth_type, override)
  url <- client$get_key_url(
    datasource$identifier,
    object,
    FALSE,
    reticulate::dict(override),
    reticulate::dict(credentials)
  )
  DominoDataR::object_http(
    "GET",
    url,
    datasource_type = datasource$datasource_type,
    write_disk = httr::write_disk(file, overwrite = TRUE),
  )
  file
}

#' Upload an object to a datasource
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to query
#' @param object The object to retrieve
#' @param what character vector, raw vector
#' @param override Configuration values to override ([add_override()])
#'
#' @return Raw vector representation of the object
#' @export
put_object <- function(client,
                       datasource,
                       object,
                       what,
                       override = list()) {
  if (is.character(what)) {
    if (length(what) > 1) {
      what <- paste(
        what,
        collapse = if (.Platform$OS.type == "unix") "\n" else "\r\n"
      )
    }
    what <- if (length(what)) charToRaw(what) else raw()
  }
  if (!is.raw(what)) {
    stop("Invalid payload of `what` - must be a raw vector or character vector")
  }

  datasource <- client$get_datasource(datasource)
  credentials <- DominoDataR::add_credentials(datasource$auth_type, override)
  url <- client$get_key_url(
    datasource$identifier,
    object,
    TRUE,
    reticulate::dict(override),
    reticulate::dict(credentials)
  )
  r <- DominoDataR::object_http(
    "PUT",
    url,
    datasource_type = datasource$datasource_type,
    request_body = what,
  )
  httr::http_status(r)$message
}

#' Upload a file to a datasource
#'
#' @param client As returned by [datasource_client()]
#' @param datasource The name of the datasource to query
#' @param object The object to retrieve
#' @param file File path to upload..
#' @param override Configuration values to override ([add_override()])
#'
#' @return Raw vector representation of the object
#' @export
upload_object <- function(client,
                          datasource,
                          object,
                          file,
                          override = list()) {
  datasource <- client$get_datasource(datasource)
  credentials <- DominoDataR::add_credentials(datasource$auth_type, override)
  url <- client$get_key_url(
    datasource$identifier,
    object,
    TRUE,
    reticulate::dict(override),
    reticulate::dict(credentials)
  )
  r <- DominoDataR::object_http(
    "PUT",
    url,
    datasource_type = datasource$datasource_type,
    request_body = httr::upload_file(file),
  )
  httr::http_status(r)$message
}
