domino_data_sources <- NULL
netapp_volumes <- NULL
.ds_cache <- new.env(hash = TRUE, parent = emptyenv())

.onLoad <- function(libname, pkgname) {
  py_select_interpreter()

  domino_data_sources <<- reticulate::import(
    "domino_data.data_sources",
    delay_load = TRUE
  )

  netapp_volumes <<- reticulate::import(
    "domino_data.netapp_volumes",
    delay_load = TRUE
  )
}
