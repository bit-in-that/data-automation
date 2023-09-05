library(dplyr)
library(tidyr)
library(fitzRoy)
library(httr)
library(arrow)
library(jsonlite)
library(purrr)

competition_metadata_afl <- read_parquet("metadata/data/processed/competition_metadata_afl.parquet") |> 
  select(competition_id = id, competition_name_lookup = name_lookup)
season_metadata_afl_extended <- read_parquet("metadata/data/processed/season_metadata_afl_extended.parquet")


season_competition_metadata <- season_metadata_afl_extended |> 
  left_join(competition_metadata_afl, "competition_id") |> 
  arrange(competition_name_lookup, year)

afl_cookie <- get_afl_cookie()


# TODO: run the code using github actions
# TODO: code for appending the existing data with this this season's data (do once a week and update) - the full thing takes too long

get_player_details <- function(
    season_code,
    afl_cookie = get_afl_cookie(),
    playerNameLike = "",
    teamId = "", # c("CD_T100","CD_T1000","CD_T40","CD_T120","CD_T60","CD_T80","CD_T10","CD_T70","CD_T30","CD_T110","CD_T20","CD_T50","CD_T1010","CD_T90","CD_T140","CD_T130","CD_T150","CD_T160")
    ageRanges = "",
    heightRanges = "",
    weightRanges = "",
    playerPosition = "", #c("KEY_FORWARD","MEDIUM_DEFENDER","MEDIUM_FORWARD","MIDFIELDER","KEY_DEFENDER","RUCK","MIDFIELDER_FORWARD")
    kickingFoot = "", # c("LEFT", "RIGHT")
    statesOfOrigin = "" #c("SA","WA","VIC","NSW","NT","QLD","INT","TAS","ACT")
    ) {
  headers <- c('x-media-mis-token' = afl_cookie)
  
  api_url <- paste0(
    "https://api.afl.com.au/statspro/playersStats/seasons/", 
    season_code, 
    "?playerNameLike=", playerNameLike,
    "&teamId=", teamId,
    "&ageRanges=", ageRanges,
    "&heightRanges=", heightRanges,
    "&weightRanges=", weightRanges,
    "&playerPosition=", playerPosition,
    "&kickingFoot=", kickingFoot,
    "&statesOfOrigin=", statesOfOrigin
    )
  
  response <- GET(url = api_url, add_headers(headers))
  content <- content(response)
  
  if(content$totalResults == 0L) {
    NULL
  } else {
    content$players |> 
      map(~{
        map_if(.x, is.list, ~list(bind_cols(.x)))
        }) |> 
      bind_rows() |> 
      unnest(all_of(c("playerDetails", "team", "totals", "averages")), names_sep = ".")
  }
}


system.time({ # should take about 3 minutes
  player_details_all <- season_competition_metadata |> 
    select(season_providerId = providerId, year, season_name = name, competition_name_lookup) |> 
    mutate(
      player_details_df = map(season_providerId, get_player_details, afl_cookie = afl_cookie)
      ) |>  
    unnest(player_details_df)
})

write_parquet(player_details_all, "players/data/raw/player_details_all.parquet")
