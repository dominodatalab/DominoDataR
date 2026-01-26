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

#' List NetApp volumes
#'
#' @param client As returned by [netapp_volume_client()]
#' @param offset Optional offset for pagination
#' @param limit Optional limit for pagination
#'
#' @return A list of RemotefsVolume objects
#' @export
list_volumes <- function(client, offset = NULL, limit = NULL) {
  if (!is.null(offset)) offset <- as.integer(offset)
  if (!is.null(limit)) limit <- as.integer(limit)
  volumes <- client$list_volumes(offset = offset, limit = limit)
  return(volumes)
}


#' List snapshots for a NetApp volume
#'
#' @param client As returned by [netapp_volume_client()]
#' @param volume_unique_name Unique name of the volume
#' @param offset Optional offset for pagination
#' @param limit Optional limit for pagination
#'
#' @return A list of RemotefsSnapshot objects
#' @export
list_snapshots <- function(client, volume_unique_name, offset = NULL, limit = NULL) {
  if (!is.null(offset)) offset <- as.integer(offset)
  if (!is.null(limit)) limit <- as.integer(limit)
  snapshots <- client$list_snapshots(
    volume_unique_name = volume_unique_name,
    offset = offset,
    limit = limit
  )
  return(snapshots)
}


#' Get a NetApp volume by name
#'
#' @param client As returned by [netapp_volume_client()]
#' @param name Unique name of the volume
#'
#' @return A Volume object
#' @export
get_volume <- function(client, name) {
  volume <- client$get_volume(name)
  return(volume)
}


#' List files in a NetApp volume
#'
#' @param client As returned by [netapp_volume_client()]
#' @param volume_unique_name Unique name of the volume
#' @param prefix Optional prefix to filter files
#' @param page_size Optional number of files to fetch
#'
#' @return A vector of file paths
#' @export
list_files <- function(client,
                               volume_unique_name,
                               prefix = "",
                               page_size = 1000) {
  files <- client$list_files(
    volume_unique_name = volume_unique_name,
    prefix = prefix,
    page_size = as.integer(page_size)
  )
  return(files)
}


#' Get URL for a file in a NetApp volume
#'
#' @param client As returned by [netapp_volume_client()]
#' @param volume_unique_name Unique name of the volume
#' @param file_name Name of the file
#'
#' @return URL string for the file
#' @export
get_file_url <- function(client, volume_unique_name, file_name) {
  url <- client$get_file_url(
    volume_unique_name = volume_unique_name,
    file_name = file_name
  )
  return(url)
}


#' Save an object from a NetApp volume to a local file
#'
#' @param client As returned by [netapp_volume_client()]
#' @param volume_unique_name Unique name of the volume
#' @param object The object to retrieve
#' @param file File path to save object at. Defaults to the object base name.
#' @param override Configuration values to override ([add_override()])
#'
#' @return File path where object was saved
#' @export
save_volume_object <- function(client,
                                volume_unique_name,
                                object,
                                file = basename(object),
                                override = list()) {
  save_object(
    client = client$datasource_client,
    datasource = volume_unique_name,
    object = object,
    file = file,
    override = override
  )
}


#' Upload content to a NetApp volume
#'
#' @param client As returned by [netapp_volume_client()]
#' @param volume_unique_name Unique name of the volume
#' @param object The object key to create/overwrite
#' @param what character vector, raw vector
#' @param override Configuration values to override ([add_override()])
#'
#' @return HTTP status message
#' @export
put_volume_object <- function(client,
                               volume_unique_name,
                               object,
                               what,
                               override = list()) {
  put_object(
    client = client$datasource_client,
    datasource = volume_unique_name,
    object = object,
    what = what,
    override = override
  )
}


#' Upload a file to a NetApp volume
#'
#' @param client As returned by [netapp_volume_client()]
#' @param volume_unique_name Unique name of the volume
#' @param object The object key in the volume
#' @param file File path to upload
#' @param override Configuration values to override ([add_override()])
#'
#' @return HTTP status message
#' @export
upload_volume_object <- function(client,
                                  volume_unique_name,
                                  object,
                                  file,
                                  override = list()) {
  upload_object(
    client = client$datasource_client,
    datasource = volume_unique_name,
    object = object,
    file = file,
    override = override
  )
}
