library(httr)
library(dplyr)
library(arrow)
library(jsonlite)
library(tidyr)
library(purrr)

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
  prospects_data <- output$draftFeeds[[1]]$prospects |> 
    bind_rows()
  selections_data <- output$draftFeeds[[1]]$selections |> 
    bind_rows() |> 
    select(-c("height", "weight", "dob", "photoUrl")) |> 
    rename(juniorClub2 = juniorClub, drafteeId2 = drafteeId) |> 
    mutate(playerId2 = playerId)
  
  full_join(prospects_data, selections_data, by = "playerId") |> 
    mutate(
      playerId = coalesce(playerId, playerId2)
      ) |> 
    select(-c("drafteeId2", "playerId2"))
  
}


get_afl_draft_prospect_seasons <- function(player_id, afl_cookie = get_afl_cookie()) {
  headers <- c("x-media-mis-token" = afl_cookie)
  response <- GET(paste0("https://api.afl.com.au/cfs/afl/draft/year/2023/prospectProfile/", player_id), add_headers(headers))
  if(response$status_code != 200L) {
    return(NULL)
  }
  output <- response |> 
    content(as = "text", encoding = "UTF-8") |> 
    fromJSON(flatten = TRUE)
  output$yearlySeasonStats$playerId <- NULL
  output |> 
    bind_cols() |> 
    as_tibble()
}


afl_draft_prospects_initial <- get_afl_draft_prospects()

afl_draft_prospects <- afl_draft_prospects_initial |> 
  mutate(
    afl_draft_prospect_seasons = map(playerId, get_afl_draft_prospect_seasons)
  ) |> 
  pull(afl_draft_prospect_seasons) |> 
  bind_rows()

# check the player ids aren't different
setdiff(combine_data_ids$playerId, afl_draft_prospects$playerId)
setdiff(afl_draft_prospects$playerId, combine_data_ids$playerId)


write_parquet(afl_draft_prospects, "players/data/processed/afl_draft_prospects.parquet")
