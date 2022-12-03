py_select_interpreter <- function() {
  # Tell {reticulate} to use the Conda version of Python available on Domino Data Lab.
  #
  # Provide other options in case the package is being used for local development.
  #
  PYTHON_PATH <- c(
    "/opt/conda/bin/python",
    path.expand("~/.virtualenvs/r-reticulate/bin/python"),
    "/usr/bin/python"
  )
  #
  for (path in PYTHON_PATH) {
    if (file.exists(path)) {
      reticulate::use_python(path)
      break
    }
  }
  #
  # If no Python is present (or not in the expected place), then install MiniConda.
  #
  if (!reticulate::py_available(initialize = TRUE)) {
    try(reticulate::install_miniconda())
  }
}

#' Install domino_data Python package
#' @return `TRUE` if installation was successful, `FALSE` otherwise.
#' @export
py_domino_data_install <- function() {
  py_select_interpreter()
  
  # Install the (Python) domino_data package.
  #
  if (!reticulate::py_module_available("domino_data")) {
    result <- tryCatch({
      reticulate::py_install("dominodatalab-data>=0.3.0,<0.4.0", pip = TRUE, method = "virtualenv")
      TRUE
    }, error = function(e) {
      FALSE
    })
    result
  } else {
    TRUE
  }
}
