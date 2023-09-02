library(dplyr)
library(tidyr)
library(fitzRoy)
library(httr)
library(arrow)
library(stringr)

competition_metadata_afl <- read_parquet("metadata/data/processed/competition_metadata_afl.parquet") |> 
  select(competition_id = id, competition_name_lookup = name_lookup)
season_metadata_afl_extended <- read_parquet("metadata/data/processed/season_metadata_afl_extended.parquet")


season_metadata_afl_extended |> 
  left_join(competition_data, "competition_id") |> 
  arrange(competition_name_lookup, year) -> season_competition_metadata

afl_cookie <- get_afl_cookie()


# TODO: turn the flatting into a function (and try to make it more efficient)
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
      toJSON() |> 
      fromJSON(flatten = TRUE) |> 
      as_tibble() |> 
      mutate(across(everything(), ~ {
        map_chr(.x, \(y) {
          if(length(y) == 0) {
            out <- NA_character_
          } else {
            as.character(y)
          }})
      })
      ) |> 
      mutate(
        across(
          starts_with("(totals|averages)\\."),
          as.numeric
        )
      ) |> 
      mutate(
        across(
          everything(), ~{
            .x_noNA <- .x[!is.na(.x)]
            if(any(str_detect(.x_noNA, "[^\\d\\.\\-]"))) {
              .x
              
            } else if(any(str_detect(.x_noNA, "\\."))) {
              as.numeric(.x)
              
            } else {
              as.integer(.x)
              
            }
          }
        )
      ) 
    
  }
}

player_details_all <- season_competition_metadata |> 
  select(season_providerId = providerId, year, season_name = name, competition_name_lookup) |> 
  mutate(
    player_details_df = map(season_providerId, get_player_details, afl_cookie = afl_cookie)
    ) |>  
  unnest(player_details_df)

write_parquet(player_details_all, "player_details/data/processed/player_details_all.parquet")

