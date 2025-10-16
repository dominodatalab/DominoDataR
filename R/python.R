#' Tell reticulate to use Python Conda version available on Domino Data Lab.
#'
#' Provide other options in case the package is used for local development.
#'
#' @return `TRUE` if a python binary was bound to reticulate, `FALSE` otherwise
#' @export
py_select_interpreter <- function() {
  PYTHON_PATH <- c(
    "/opt/conda/bin/python",
    path.expand("~/.virtualenvs/r-reticulate/bin/python"),
    "/usr/bin/python"
  )
  #
  for (path in PYTHON_PATH) {
    if (file.exists(path)) {
      reticulate::use_python(path)
      return(TRUE)
    }
  }
  return(FALSE)
}

#' Install domino_data Python package
#'
#' @return `TRUE` if installation was successful, `FALSE` otherwise.
#' @param version Version of the domino_data package to install.
#' @export
py_domino_data_install <- function(version) {
  py_select_interpreter()

  # Install the (Python) domino_data package.
  #
  if (!reticulate::py_module_available("domino_data")) {
    if (missing(version)) {
      package <- "dominodatalab-data"
    } else {
      package <- paste0("dominodatalab-data==", version)
    }
    result <- tryCatch(
      {
        reticulate::py_install(package, pip = TRUE, method = "virtualenv")
        TRUE
      },
      error = function(e) {
        FALSE
      }
    )
    result
  } else {
    TRUE
  }
}
