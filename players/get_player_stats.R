library(httr)
library(arrow)
library(tidyr)
library(dplyr)
library(purrr)
library(stringr)

source("players/modules/get_afl_token.R")
afl_token <- get_afl_token()
match_metadata_afl <- read_parquet("metadata/data/processed/match_metadata_afl.parquet")
teams_names <- read_parquet("metadata/data/processed/teams_metadata_afl.parquet") |> 
  select(team_name = name, teamId = providerId)

convert_player_stats <- function(player_stats_list) {
  
  player_stats_list |> 
    list_flatten() |> list_flatten() |> list_flatten() |> list_flatten() |> 
    map_if(is.null, ~NA) |> 
    as_tibble() |>
    select(!starts_with("player_player_player_")) |>
    select(-playerStats_lastUpdated, -teamId) |> 
    rename_with(str_remove, pattern = ".*_")
  
}

get_match_player_stats <- function(match_id, convert_to_df = TRUE) {
  
  headers = c(
    "x-media-mis-token" = afl_token
  )
  
  response <- GET(url = paste0("https://api.afl.com.au/cfs/afl/playerStats/match/", match_id), 
                  add_headers(headers))
  output <- content(response)
  
  if(convert_to_df) {
    output <- c(
      map(output$homeTeamPlayerStats, convert_player_stats),
      map(output$awayTeamPlayerStats, convert_player_stats)
    ) |> 
      bind_rows()
  }
  
  output
}

player_stats_all <- match_metadata_afl |>
  select(competition_id, season_id, round_id, match_id = providerId, year, roundNumber, status, is_final, home_team_name, away_team_name) |> 
  filter(status == "CONCLUDED") |> 
  slice(c(1, 2447, 2448, 2710, 2709, 3165, 3166, 3462, 3463, 3574, 3575, 3778, 4282:4284, 4555, 4556, 4843, 4844, 4953, 5144)) |> 
  slice(1:4) |> 
  mutate(
    output = map(match_id, get_match_player_stats)
  ) |> 
  unnest(output) |> 
  bind_rows() |> 
  left_join(teams_names, by = "teamId") |> 
  relocate(playerId, givenName, surname, team_name , .after = is_final)
  
write_parquet(player_stats_all, "players/data/raw/player_stats_all.parquet")
