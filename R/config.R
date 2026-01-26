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

#' Add override configuration values
#'
#' @param ... named configuration values.
#' @param .override a named character vector
#'
#' @return named list of override configuration values
#' @export
add_override <- function(..., .override = character()) {
  override <- c(..., .override)
  if (!is.null(override)) {
    stopifnot(is.character(override))
  }
  as.list(override)
}

#' Add credentials override for the right datasources
#'
#' @param auth_type Datasource Authentication type
#' @param config existing config override to extend
#'
#' @return named list of override configuration values
#' @export
add_credentials <- function(auth_type, config = list()) {
  if (auth_type == "OAuth") {
    credentials <- load_oauth_credentials(
      "DOMINO_API_PROXY",
      "/access-token"
    )
  } else if (is.element(auth_type, c("AWSIAMRole", "AWSIAMRoleWithUsername"))) {
    credentials <- load_aws_credentials(
      Sys.getenv(
        "AWS_SHARED_CREDENTIALS_FILE",
        "/var/lib/domino/home/.aws/credentials"
      ),
      config[["profile"]]
    )
  } else {
    credentials <- list()
  }
  append(config, credentials)
}

#' Load AWS Credentials
#'
#' @param location file path where AWS credentials are located
#' @param profile AWS profile or section to use
#'
#' @return named list of override configuration values
load_aws_credentials <- function(location, profile = NULL) {
  c <- ConfigParser::ConfigParser$new()
  c$read(location)

  list(
    accessKeyID = c$get("aws_access_key_id", NULL, section = profile),
    secretAccessKey = c$get("aws_secret_access_key", NULL, section = profile),
    sessionToken = c$get("aws_session_token", NULL, section = profile)
  )
}

#' Load OAuth Credentials
#'
#' @param env_var the environment variable name containing the API proxy URL
#' @param path the API endpoint path for retrieving the token
#'
#' @return named list of override configuration values
load_oauth_credentials <- function(env_var, path) {
  api_proxy <- Sys.getenv(env_var)
  if (api_proxy == "") {
    stop(env_var, " environment variable is not set")
  }
  
  url <- paste0(api_proxy, path)
  
  tryCatch({
    response <- httr::GET(url)
    httr::stop_for_status(response)
    token <- httr::content(response, "text", encoding = "UTF-8")
    list(token = token)
  }, error = function(e) {
    stop("Failed to retrieve token from ", url, ": ", e$message)
  })
}
