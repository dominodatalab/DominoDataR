skip_if_not_domino_data <- function() {
  have_domino_data <- reticulate::py_module_available("domino_data")
  if (!have_domino_data) {
    testthat::skip("Domino Data library is not available for testing")
  }
}

test_that("datasource client can be initialized", {
  skip_if_not_domino_data()

  token_file <- tempfile()
  writeLines("TOKEN", token_file)

  expect_no_error(DominoDataR::datasource_client(api_key = "API_KEY"))
  expect_no_error(DominoDataR::datasource_client(token_file = token_file))
  expect_no_error(DominoDataR::datasource_client(token_url = "TOKEN_URL"))
  expect_no_error(DominoDataR::datasource_client(token = "TOKEN"))
  expect_no_error(DominoDataR::datasource_client())
})

test_that("netapp volume client can be initialized", {
  skip_if_not_domino_data()

  token_file <- tempfile()
  writeLines("TOKEN", token_file)

  expect_no_error(DominoDataR::netapp_volume_client(token_file = token_file))
  expect_no_error(DominoDataR::netapp_volume_client(token = "TOKEN"))
})
