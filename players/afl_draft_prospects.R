library(httr)
library(dplyr)
library(arrow)
library(jsonlite)
library(tidyr)

combine_data_ids <- read_parquet("players/data/raw/combine_data_ids.parquet")

get_afl_cookie <- function() {
  
  response <- POST("https://api.afl.com.au/cfs/afl/WMCTok")
  token <- content(response)$token
  
  return(token)
}

get_afl_draft_prospects <- function(afl_cookie = get_afl_cookie()) {
  headers <- c("x-media-mis-token" = afl_cookie)
  response <- GET("https://api.afl.com.au/cfs/afl/draft/year/2023", add_headers(headers))
  stopifnot(response$status_code == 200L)
  output <- content(response)
  output$draftFeeds[[1]]$prospects |> 
    bind_rows()
}


get_afl_draft_prospect_seasons <- function(player_id, afl_cookie = get_afl_cookie()) {
  headers <- c("x-media-mis-token" = afl_cookie)
  response <- GET(paste0("https://api.afl.com.au/cfs/afl/draft/year/2023/prospectProfile/", player_id), add_headers(headers))
  stopifnot(response$status_code == 200L)
  output <- response |> 
    content(as = "text", encoding = "UTF-8") |> 
    fromJSON(flatten = TRUE)
  output$yearlySeasonStats$playerId <- NULL
  output |> 
    bind_cols() |> 
    as_tibble()
}


afl_draft_prospects_initial <- get_afl_draft_prospects()

# check the player ids aren't different
setdiff(combine_data_ids$playerId, afl_draft_prospects$playerId)
setdiff(afl_draft_prospects$playerId, combine_data_ids$playerId)

afl_draft_prospects <- afl_draft_prospects_initial |> 
  mutate(
    afl_draft_prospect_seasons = map(playerId, get_afl_draft_prospect_seasons)
  ) |> 
  pull(afl_draft_prospect_seasons) |> 
  bind_rows()


write_parquet(afl_draft_prospects, "players/data/processed/afl_draft_prospects.parquet")
