if (!nzchar(Sys.getenv('CI'))) {
  source("R/python.R")
  
  py_domino_data_install()
}
