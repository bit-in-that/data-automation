library(httr)

get_afl_token <- function () {
  response <- POST("https://api.afl.com.au/cfs/afl/WMCTok")
  content(response)$token
}
