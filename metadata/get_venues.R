library(httr)
library(dplyr)
library(purrr)
library(arrow)

get_venues <- function(page = NULL, pageSize = 100, compSeasonId = NULL) {
  query = list(
    page = page, 
    pageSize = pageSize,
    compSeasonId = compSeasonId
  )
  
  response <- GET("https://aflapi.afl.com.au/afl/v2/venues", query = query)
  output <- content(response)
}
venue_info_first <- get_venues()

page_count <- venue_info_first$meta$pagination$numPages

venues_metadata_afl <- seq(page_count-1) |> 
  map(~ get_venues(.x)$venues) |> 
  c(venue_info_first$venues) |> 
  bind_rows()

write_parquet(venues_metadata_afl, "metadata/data/processed/venues_metadata_afl.parquet")
