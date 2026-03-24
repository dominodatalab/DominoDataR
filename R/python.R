#' Tell reticulate to use Python Conda version available on Domino Data Lab.
#'
#' Provide other options in case the package is used for local development.
#'
#' @return `TRUE` if a python binary was bound to reticulate, `FALSE` otherwise
#' @export
py_select_interpreter <- function() {
  # Build candidate list in priority order:
  #   1. RETICULATE_PYTHON — explicit user/system choice, highest priority
  #   2. VIRTUAL_ENV/bin/python — user has a deliberate venv active
  #   3. Domino/system defaults
  env_python  <- Sys.getenv("RETICULATE_PYTHON", unset = "")
  venv_root   <- Sys.getenv("VIRTUAL_ENV",       unset = "")
  PYTHON_PATH <- c(
    if (nzchar(env_python)) env_python else character(0),
    if (nzchar(venv_root))  file.path(venv_root, "bin", "python") else character(0),
    "/opt/conda/bin/python",
    path.expand("~/.virtualenvs/r-reticulate/bin/python"),
    "/usr/bin/python3",
    "/usr/bin/python"
  )
  for (path in PYTHON_PATH) {
    if (file.exists(path)) {
      # Clear VIRTUAL_ENV so reticulate doesn't try to re-interpret it after we
      # have already resolved the Python to use.  We pin RETICULATE_PYTHON
      # instead, which reticulate respects unconditionally.  This also means
      # users never need to call Sys.unsetenv("VIRTUAL_ENV") manually before
      # library(DominoDataR) — but if they set VIRTUAL_ENV intentionally it
      # was already honoured above (position 2 in the candidate list).
      Sys.unsetenv("VIRTUAL_ENV")
      Sys.setenv(RETICULATE_PYTHON = path)

      # Prepend the Python install's lib dir to LD_LIBRARY_PATH so the dynamic
      # linker finds the correct OpenSSL (and other bundled shared libs) when
      # reticulate loads libpython.  Without this, the system libcrypto.so.3 may
      # be too old for the SSL extension the Python was compiled against
      # (e.g. OPENSSL_3.3.0 required but only 3.0.x present in /usr/lib).
      #
      # For virtualenvs the SSL libraries live in the *base* Python's prefix,
      # not the venv's own prefix.  pyvenv.cfg always exists in the venv root
      # and its `home` key gives the base Python's bin dir, so we follow it.
      python_prefix <- dirname(dirname(path))
      venv_cfg      <- file.path(python_prefix, "pyvenv.cfg")
      lib_prefix    <- if (file.exists(venv_cfg)) {
        lines     <- readLines(venv_cfg, warn = FALSE)
        home_line <- grep("^home\\s*=", lines, value = TRUE)
        if (length(home_line) > 0L) {
          home_dir <- trimws(sub("^home\\s*=\\s*", "", home_line[[1L]]))
          dirname(home_dir)   # <base_prefix>/bin  →  <base_prefix>
        } else {
          python_prefix       # pyvenv.cfg exists but no home key — fall back
        }
      } else {
        python_prefix         # not a venv; use the binary's own prefix
      }
      python_lib <- file.path(lib_prefix, "lib")
      if (dir.exists(python_lib)) {
        current_ldpath <- Sys.getenv("LD_LIBRARY_PATH", unset = "")
        if (!grepl(python_lib, current_ldpath, fixed = TRUE)) {
          Sys.setenv(LD_LIBRARY_PATH = if (nzchar(current_ldpath)) {
            paste0(python_lib, ":", current_ldpath)
          } else {
            python_lib
          })
        }
      }

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
