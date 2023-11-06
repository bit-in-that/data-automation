library(dplyr)
library(tidyr)
library(arrow)
library(stringr)
library(purrr)


wafl_id_conversion_table <- read_parquet("state_leagues/data/processed/wafl_id_conversion_table.parquet")
match_metadata_afl <- read_parquet("metadata/data/processed/match_metadata_afl.parquet")
competition_metadata_afl <- read_parquet("metadata/data/processed/competition_metadata_afl.parquet")
season_metadata_afl <- read_parquet("metadata/data/processed/season_metadata_afl.parquet")

round_metadata_afl <- read_parquet("metadata/data/processed/round_metadata_afl.parquet")
match_metadata_afl <- read_parquet("metadata/data/processed/match_metadata_afl.parquet")

player_details_sanfl <- read_parquet("state_leagues/data/raw/player_details_sanfl.parquet")

sanfl_season_finals_info <- read_parquet("state_leagues/data/raw/sanfl_season_finals_info.parquet")

wafl_player_stats <- read_parquet("state_leagues/data/raw/wafl_player_stats.parquet")

player_details_wafl <- read_parquet("state_leagues/data/raw/player_details_wafl.parquet")

u18_champs_player_stats <- read_parquet("state_leagues/data/raw/u18_champs_player_stats.parquet")

player_details_all <- read_parquet("players/data/raw/player_details_all.parquet")

source("state_leagues/modules/get_wafl_data.R")

# used:
combine_data_ids <- read_parquet("players/data/raw/combine_data_ids.parquet")
afl_combine_data_ids <- combine_data_ids |> 
  select(-team_name) |> 
  left_join(
    wafl_id_conversion_table |> select(official_id, wafl_id),
    by = c("playerId" = "official_id")
  ) |> 
  rename(
    playerId_wafl = wafl_id
  ) |> 
  mutate(
    playerId_sanfl = if_else(str_detect(STATE, "SA"), str_remove(playerId, "^CD_I"), NA_character_)
  )

aflw_combine_data_ids <- read_parquet("players/data/raw/aflw_combine_data_ids.parquet")
player_stats_all <- read_parquet("players/data/raw/player_stats_all.parquet")


# Prepare data for different competitions ----
## SANFL ----

sanfl_team_map <- read_parquet("state_leagues/data/raw/sanfl_team_map.parquet")
sanfl_player_stats <- read_parquet("state_leagues/data/raw/sanfl_player_stats.parquet") |> 
  mutate(
    venueTimezone = coalesce(venueTimezone, "Australia/Adelaide"),
    localStartTime = localStartTime |> str_remove("\\+.*$") |> str_replace("T", " ")
  )



sanfl_player_time_map <- sanfl_player_stats |> 
  select(localStartTime, venueTimezone) |> 
  distinct() |> 
  mutate(
    match_time = map2_vec(localStartTime, venueTimezone, as.POSIXct, format = "%Y-%m-%d %H:%M:%S")
  )

sanfl_player_stats_cut <- sanfl_player_stats |> 
  left_join(sanfl_player_time_map, by = c("localStartTime", "venueTimezone")) |>
  left_join(sanfl_team_map, by = c("current_squadId" = "squadId"), suffix = c("", "_current")) |>
  left_join(sanfl_season_finals_info |> transmute(roundNumber, comp_key, season = as.integer(season_key), finals_shortName), 
            by = c("leagueCode" = "comp_key", "season", "roundNumber")) |> 
  transmute(
    player_id, player_url = paste0("https://sanfl.com.au/league/clubs/", club_slug, "/", player_id), player_image = image, 
    tier = case_when(
      leagueCode %in% c("sanfl", "state", "womens", "vflw") ~ "State League",
      leagueCode %in% c("reserves") ~ "State Reserves",
      leagueCode %in% c("u18", "u16") ~ "State Underage",
      TRUE ~ NA_character_
      ), 
    comp_name = case_when(
      leagueCode == "sanfl" ~ "SANFL League",
      leagueCode == "vflw" ~ "VFLW",
      leagueCode == "state" ~ "Interstate",
      TRUE ~ paste("SANFL", str_to_title(leagueCode))
    ), 
    season, match_id = matchId, match_url = paste0("https://sanfl.com.au/league/matches/", matchId), 
    match_time, match_type = case_when(
      leagueCode == "state" ~ "Special",
      str_length(roundNumber) == 3 ~ "Final",
      TRUE ~ "Regular"
    ),
    round_abbreviation = case_when(
      is.na(roundNumber) ~ "?",
      match_type == "Final" & is.na(finals_shortName) ~ "?",
      match_type == "Final" ~ finals_shortName,
      TRUE ~ paste("Rd", roundNumber)
    ), 
    is_home = (squadId == homeSquadId), team_name = if_else(is_home, homeSquadName, awaySquadName), 
    opposition_name = if_else(is_home, awaySquadName, homeSquadName), position = NA_character_,
    fantasy_points = goals*6 + behinds + kicks*3 + handballs*2 + marks*3 + tackles*4 + hitouts + freesFor - freesAgainst*3,
    goals, behinds, kicks, handballs, disposals, marks, tackles, hitouts, frees_for = freesFor, frees_against = freesAgainst, 
    # inside50s, rebound50s, clearances, kickins, marksContested, kickIneffective, handballsReceived
  )

## VIC U18 ----

player_stats_all_cut <- player_stats_all |> 
  left_join(competition_metadata_afl |> select(competition_id = id, competition_code = code, competition_name = name), 
            by = c("competition_id")) |> 
  left_join(match_metadata_afl |> select(match_id = providerId, match_index = id, match_time = match_start_time), 
            by = c("match_id")) |> 
  left_join(round_metadata_afl |> select(round_id = id, round_abbreviation = abbreviation), 
            by = c("round_id")) |> 
  # filter(!competition_code %in% c("WAFL", "SANFL")) |> # remove these as they come from a different data source
  transmute(
    player_id = playerId, player_url = paste0("https://www.afl.com.au/stats/players?playerId=", playerId), player_image = photoURL,
    tier = case_when(
      competition_code %in% c("AFL", "AFLW") ~ "National",
      competition_code %in% c("VFL", "VFLW", "WAFL", "SANFL") ~ "State League",
      competition_code %in% c("U18B", "U18G") ~ "State Underage",
      TRUE ~ NA_character_
    ),
    comp_name = case_when(
      competition_code %in% c("WAFL", "SANFL") ~ paste(competition_code, "League"),
      TRUE ~ competition_code
    ),
    season = year, match_id, match_url = paste0("https://www.afl.com.au/afl/matches/", match_index),
    match_time, match_type = case_when(
      competition_name == "State of Origin" ~ "Special",
      competition_name == "AFL Preseason" ~ "Preseason",
      is_final ~ "Final",
      TRUE ~ "Regular"
    ),
    round_abbreviation,
    is_home = (team_name == home_team_name), team_name = if_else(is_home, home_team_name, away_team_name),
    opposition_name = if_else(is_home, away_team_name, home_team_name), position,
    fantasy_points = goals*6 + behinds + kicks*3 + handballs*2 + marks*3 + tackles*4 + hitouts + freesFor - freesAgainst*3,
    goals, behinds, kicks, handballs, disposals, marks, tackles, hitouts, frees_for = freesFor, frees_against = freesAgainst
  )

## WAFL ----

wafl_player_stats_women_combine <- aflw_combine_data_ids |> 
  filter(STATE == "WA") |> 
  left_join(player_details_wafl |> transmute(playerId_wafl = id, wafl_url = paste0("https://wafl.com.au/player/", slug)), 
            by = "playerId_wafl") |> 
  mutate(
    wafl_data = map(playerId_wafl, get_wafl_player_stats)
  ) |> 
  select(wafl_id = playerId_wafl, wafl_url, wafl_data) |> 
  unnest(wafl_data) |> 
  filter(
    match.season.name > 2021, match.competition.name == "WAFLW" # there are some wafl reserves, preseason and rogers cup (all have goal stats only)
  )

wafl_image_data <- afl_combine_data_ids |> 
  bind_rows(aflw_combine_data_ids) |>
  select(wafl_id = playerId_wafl) |> 
  filter(!is.na(wafl_id)) |> 
  left_join(player_details_wafl |> select(id, slug), 
            by = c("wafl_id" = "id")) |> 
  mutate(
    player_image = map_chr(slug, get_wafl_player_image)
  )

wafl_player_stats_cut <- wafl_player_stats |> 
  bind_rows(wafl_player_stats_women_combine) |> 
  left_join(wafl_image_data, by = "wafl_id") |> 
  transmute(
    player_id = wafl_id, player_url = wafl_url, player_image,
    tier = case_when(
      str_detect(match.competition.name, "League") | match.competition.name %in% c("WAFLW", "National", "State") ~ "State League",
      str_detect(match.competition.name, "Reserves") ~ "State Reserves",
      str_detect(match.competition.name, "Colts|Rogers Cup") | match.competition.name %in% c("Futures") ~ "State Underage",
      TRUE ~ NA_character_
    ),
    comp_name = str_remove(match.competition.name, "^Preseason "),
    comp_name = case_when(
      comp_name %in% c("Reserves", "Colts", "League", "Futures") ~ paste("WAFL", comp_name),
      comp_name %in% c("WAFLW") ~ paste(comp_name, "League"),
      comp_name %in% c("Rogers Cup") ~ paste("WAFLW", comp_name),
      comp_name %in% c("State") ~ "Interstate",
      comp_name %in% c("National") ~ "Foxtel Cup",
      TRUE ~ comp_name
    ),
    season = match.season.name, match_id, match_url = paste0("https://wafl.com.au/match/", match.slug),
    match_time = as.POSIXct(match.utc_datetime, format = "%Y-%m-%d %H:%M:%S", tz = "utc"), 
    match_type = case_when(
      match.round.name %in% c("State Game", "Foxtel Cup") ~ "Special",
      str_detect(match.competition.name, "^Preseason ") ~ "Preseason",
      str_detect(match.round.name, "Final") ~ "Final",
      TRUE ~ "Regular"
    ),
    round_abbreviation = case_when(
      match_type == "Special" ~ "?",
      str_detect(match.round.name, "Semi Final") ~ "SF",
      match_type == "Final" ~ match.round.name |> str_extract_all("\\b\\w") |> map_chr(paste0, collapse = ""),
      TRUE ~ str_replace(match.round.name, "Round", "Rd")
    ),
    is_home = (club.name == match.home.name), team_name = club.name,
    opposition_name = if_else(is_home, match.away.name, match.home.name), position = NA_character_,
    fantasy_points = goals*6 + behinds + kicks*3 + handballs*2 + marks*3 + tackles*4 + hitouts + frees_for - frees_against*3,
    goals, behinds, kicks, handballs, disposals, marks, tackles, hitouts, frees_for, frees_against
  )

## U18 interstate championship ----


u18_champs_player_stats |> 
  distinct(Player, team_name) |> 
  mutate(
    last_name = case_when(
      str_detect(Player, regex("\\b van \\b", ignore_case = TRUE)) ~ str_extract(Player, regex("\\b van \\b.*", ignore_case = TRUE)),
      str_detect(Player, regex("\\b de \\b", ignore_case = TRUE)) ~ str_extract(Player, regex("\\b de \\b.*", ignore_case = TRUE)),
      str_detect(Player, regex("\\bJr$", ignore_case = TRUE)) ~ str_extract(Player, regex(" \\S+ Jr$", ignore_case = TRUE)),
      TRUE ~ str_extract(Player, " \\S+$")
    ) |> 
      str_trim(),
    first_name = str_remove(Player, paste0(" ", last_name, "$"))
  ) |> 
  # filter(str_count(Player, "\\s") > 1) |> 
  View()
  


# TODO: add the U18 champs numbers here too
# TODO: add birth date and age as at 31 December 2023


player_details_wafl |> 
  head() |> 
  pull(slug) |> 
  map_chr(get_wafl_player_image)


wafl_url_mapping <- player_details_wafl |> 
  transmute(
    playerId_wafl = id,
    wafl_url = paste0("https://wafl.com.au/player/", slug)
  )


aflw_combine_data_ids |> 
  left_join(wafl_url_mapping, by = "playerId_wafl")



combine_data_ids |> 
  select(-team_name) |> 
  left_join(
    wafl_id_conversion_table,
    by = c("playerId" = "official_id")
  ) |> 
  rename(
    playerId_wafl = wafl_id
  ) |> 
  mutate(
    playerId_sanfl = if_else(str_detect(STATE, "SA"), str_remove(playerId, "^CD_I"), NA_character_),
    player_stats_sanfl = map(playerId_sanfl, ~{
      if(is.na(.x)) {
        NULL
      } else {
        sanfl_player_stats_cut |> 
          filter(player_id == .x)        
      }
    })
  ) |> 
  # unnest(player_stats_sanfl) |> 
  View()

# clearances = totalClearances

player_stats_all |> 
  filter(status == "CONCLUDED") |> 
  left_join(select(match_metadata_afl, match_index = id, match_id = providerId), by = "match_id") |> 
  select(
    playerId, givenName, surname, playerJumperNumber, team_name, match_index, year,
    goals, behinds, disposals, kicks, handballs, marks, tackles, hitouts, freesFor, freesAgainst, dreamTeamPoints
  ) |> 
  mutate(
    fantasy_points = goals*6 + behinds + kicks*3 + handballs*2 + marks*3 + tackles*4 + hitouts + freesFor - freesAgainst*3,
    dt_diff = fantasy_points - dreamTeamPoints
  ) |> 
  filter(dt_diff != 0) |> View()

# TODO: get the player stats for U18

