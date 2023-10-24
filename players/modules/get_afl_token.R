get_afl_token <- function () {
  response <- httr::POST("https://api.afl.com.au/cfs/afl/WMCTok")
  content(response)$token
}
