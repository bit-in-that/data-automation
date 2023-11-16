library(dplyr)
library(tidyr)
library(arrow)
library(stringr)
library(purrr)

source("state_leagues/modules/get_wafl_data.R")
na_rm <- function(x) {
  x[!is.na(x)]
}

competition_metadata_afl <- read_parquet("metadata/data/processed/competition_metadata_afl.parquet")
round_metadata_afl <- read_parquet("metadata/data/processed/round_metadata_afl.parquet")
match_metadata_afl <- read_parquet("metadata/data/processed/match_metadata_afl.parquet")

sanfl_season_finals_info <- read_parquet("state_leagues/data/raw/sanfl_season_finals_info.parquet")
sanfl_team_map <- read_parquet("state_leagues/data/raw/sanfl_team_map.parquet")

wafl_id_conversion_table <- read_parquet("state_leagues/data/processed/wafl_id_conversion_table.parquet")
wafl_player_stats <- read_parquet("state_leagues/data/raw/wafl_player_stats.parquet")
player_details_wafl <- read_parquet("state_leagues/data/raw/player_details_wafl.parquet")

u18_champs_player_stats <- read_parquet("state_leagues/data/raw/u18_champs_player_stats.parquet")

player_details_all <- read_parquet("players/data/raw/player_details_all.parquet")
player_stats_all <- read_parquet("players/data/raw/player_stats_all.parquet")

phantom_draft_rankings <- read_parquet("state_leagues/data/raw/phantom_draft_rankings.parquet")

combine_data_ids <- read_parquet("players/data/raw/combine_data_ids.parquet")
aflw_combine_data_ids <- read_parquet("players/data/raw/aflw_combine_data_ids.parquet")


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


# Prepare data for different competitions ----
## SANFL ----
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
  left_join(sanfl_season_finals_info |> transmute(roundNumber, comp_key, season = as.integer(season_key), finals_shortName), 
            by = c("leagueCode" = "comp_key", "season", "roundNumber")) |> 
  transmute(
    playerId = player_id,
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
    inside50s, rebound50s, clearances, contested_possession = NA_integer_, uncontested_possession = NA_integer_, clearances = NA_integer_,
    contested_marks = marksContested, ineffective_kicks = kickIneffective, kickins, handballs_received = handballsReceived
  )

## VIC U18 ----

player_stats_all_cut <- player_stats_all |> 
  left_join(competition_metadata_afl |> select(competition_id = id, competition_code = code, competition_name = name), 
            by = c("competition_id")) |> 
  left_join(match_metadata_afl |> select(match_id = providerId, match_index = id, match_time = match_start_time), 
            by = c("match_id")) |> 
  left_join(round_metadata_afl |> select(round_id = id, round_abbreviation = abbreviation), 
            by = c("round_id")) |> 
  transmute(
    playerId,
    tier = case_when(
      competition_code %in% c("AFL", "AFLW") ~ "National",
      competition_code %in% c("VFL", "VFLW", "WAFL", "SANFL") ~ "State League",
      competition_code %in% c("U18B", "U18G") ~ "State Underage",
      TRUE ~ NA_character_
    ),
    comp_name = case_when(
      competition_code %in% c("WAFL", "SANFL") ~ paste(competition_code, "League"),
      competition_code %in% c("U18B") ~ "Vic U18 Boys",
      competition_code %in% c("U18G") ~ "Vic U18 Girls",
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
    goals, behinds, kicks, handballs, disposals, marks, tackles, hitouts, frees_for = freesFor, frees_against = freesAgainst,
    inside50s, rebound50s, clearances = NA_integer_, contested_possession = NA_integer_, uncontested_possession = NA_integer_, clearances = NA_integer_,
    contested_marks = NA_integer_, ineffective_kicks = NA_integer_, kickins = NA_integer_, handballs_received = NA_integer_
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

wafl_player_stats_cut <- wafl_player_stats |> 
  bind_rows(wafl_player_stats_women_combine) |> 
  transmute(
    playerId = wafl_id,
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
    fantasy_points = coalesce(goals, 0)*6 + behinds + kicks*3 + handballs*2 + marks*3 + tackles*4 + hitouts + frees_for - frees_against*3,
    goals = coalesce(goals, 0), behinds, kicks, handballs, disposals, marks, tackles, hitouts, frees_for, frees_against,
    inside50s = NA_integer_, rebound50s = NA_integer_, clearances = NA_integer_, contested_possession = NA_integer_, uncontested_possession = NA_integer_, clearances = NA_integer_,
    contested_marks = NA_integer_, ineffective_kicks = NA_integer_, kickins = NA_integer_, handballs_received = NA_integer_
  ) |> 
  filter(
    comp_name != "WAFL Futures", match_type != "Preseason"
  )

## U18 interstate championship ----
combine_players_both <- bind_rows(
  afl_combine_data_ids |> mutate(gender = "male"),
  aflw_combine_data_ids |> mutate(gender = "female")
) |> 
  mutate(capitalised_name = paste(NAME, SURNAME) |> str_to_upper())

u18_champs_player_stats_mapped <- u18_champs_player_stats |> 
  mutate(
    Player2 = Player |> 
      str_replace("’", "'") |> 
      str_replace("^Mitch\\b", "Mitchell") |> 
      str_replace("^Will\\b", "William")
      ,
    capitalised_name = case_when(
      Player2 == "Matt Carroll" ~ "Matthew Carroll",
      Player2 == "Harry De Mattia" ~ "Harry DeMattia",
      Player2 == "Mitch Edwards" ~ "Mitchell Edwards",
      Player2 == "Ollie Murphy" ~ "Oliver Murphy",
      Player2 == "Cam Nyko" ~ "Cameron Nyko",
      Player2 == "William Brown" ~ "Will Brown",
      Player2 == "Charlie Harrop" ~ "Charlton Harrop",
      Player2 == "Gabby Eaton" ~ "Gabrielle Eaton",
      Player2 == "Keely Fullerton" ~ "Keeley Fullerton",
      Player2 == "T'Sharni Graham" ~ "TSharni Graham",
      Player2 == "Asha Turner Funk" ~ "Asha Turner-Funk",
      TRUE ~ Player2
      ) |> 
      str_to_upper()
  ) |> 
  select(-Player2) |> 
  left_join(
    combine_players_both |> distinct(capitalised_name, playerId, .keep_all = TRUE),
    by = "capitalised_name"
  ) |> 
  mutate(
    playerId_combined = coalesce(playerId, playerId_wafl)
  )

u18_champs_player_stats_cut <-  u18_champs_player_stats_mapped |> 
  transmute(
    playerId = playerId_combined,
    tier = "Interstate Underage",
    comp_name = if_else(League == "Trial Matches", "AFL U18 Championships", League),
    season = Season, match_id = NA_character_, match_url,
    match_time, match_type = case_when(
      League == "Trial Matches" ~ "Special",
      TRUE ~ "Regular"
    ),
    round_abbreviation = NA_character_,
    is_home = (team_name == home_Team), team_name,
    opposition_name = if_else(is_home, away_Team, home_Team), position = Position,
    fantasy_points = GL*6 + 0 + K*3 + HB*2 + M*3 + `T`*4 + HO + 0 - 0*3,
    goals = GL, behinds = NA_integer_, kicks = K, handballs = HB, disposals = D, marks = M, 
    tackles  =`T`, hitouts = HO, frees_for = NA_integer_, frees_against = NA_integer_,
    inside50s = I50, rebound50s = R50, clearances = CLR, contested_possession = CP, uncontested_possession = UP, clearances = NA_integer_,
    contested_marks = NA_integer_, ineffective_kicks = NA_integer_, kickins = NA_integer_, handballs_received = NA_integer_
  )


# Combine everything together ----
player_stats_cut_combined <- bind_rows(
  player_stats_all_cut,
  wafl_player_stats_cut,
  sanfl_player_stats_cut,
  u18_champs_player_stats_cut
)

combine_player_stats <- bind_rows(
  combine_players_both |> select(-playerId_wafl, -playerId_sanfl, playerId = playerId, playerId_combined = playerId) |> filter(!is.na(playerId_combined)),
  combine_players_both |> select(-playerId_wafl, playerId_combined = playerId_sanfl) |> filter(!is.na(playerId_combined)),
  combine_players_both |> select(-playerId_sanfl, playerId_combined = playerId_wafl) |> filter(!is.na(playerId_combined))
) |> 
  select(
    playerId = playerId_combined, player_first_name = NAME, player_surname = SURNAME#, player_state = STATE#, state_league_club = `STATE LEAGUE CLUB`, community_club = `COMMUNITY CLUB`
  ) |>
  left_join(
    player_stats_cut_combined,
    by = c("playerId"),
    relationship = "many-to-many"
  ) |>
  filter(!is.na(match_time)) |> 
  distinct(player_first_name, player_surname, match_time, .keep_all = TRUE)

combine_player_seasons <- combine_player_stats |> 
  group_by(
    playerId, tier, comp_name, season
  ) |> 
  summarise(
    games_played = n(),
    fantasy_ceiling = max(fantasy_points),
    fantasy_floor = min(fantasy_points),
    across(fantasy_points:frees_against, mean),
    .groups = "drop"
  ) |> 
  mutate(
    tier_short = case_when(
      tier == "Interstate Underage" ~ "interstate_underage",
      tier == "State League" ~ "state_league",
      tier == "State Reserves" ~ "state_reserves",
      tier == "State Underage" ~ "state_underage"
    )
  )

player_images_metadata <- player_stats_all |> 
  left_join(competition_metadata_afl, by = c("competition_id" = "id")) |> 
  arrange(desc(year), code) |> 
  filter(
    playerId %in% unique(combine_players_both$playerId)
  ) |> 
  bind_rows(
    player_details_all |> filter(playerId %in% unique(combine_players_both$playerId)) |> select(playerId, playerDetails.photoURL)
  ) |>
  distinct(playerId, player_image = photoURL) |> 
  group_by(playerId) |> 
  summarise(
    player_images_afl = list(unique(player_image))
  )

player_metadata_afl <- player_details_all |> 
  filter(
    playerId %in% unique(combine_players_both$playerId)
  ) |> 
  transmute(
    playerId, #first_name = playerDetails.givenName, surname = playerDetails.surname, 
    date_of_birth = playerDetails.dateOfBirth,
    player_height = playerDetails.heightCm, player_weight = playerDetails.weightKg, year,
  ) |> 
  arrange(desc(year)) |> 
  group_by(playerId) |>
  summarise(
    # first_name = head(first_name, 1),
    # surname = head(surname, 1),
    date_of_birth = date_of_birth |> table() |> which.max() |> names(),
    player_height_max = max(player_height),
    player_height_min = min(player_height),
    player_weight_max = max(player_weight),
    player_weight_min = min(player_weight),
    .groups = "drop"
  ) |> mutate(
    player_height_min = if_else(player_height_min == 0L, player_height_max, player_height_min),
    player_height_range = if_else(player_height_max == player_height_min, as.character(player_height_max), paste(player_height_min, player_height_max, sep = "-")),
    player_weight_max = if_else(player_weight_max == 0L, NA_integer_, player_weight_max),
    player_weight_min = if_else(player_weight_min == 0L, NA_integer_, player_weight_min),
    player_weight_range = if_else(player_weight_max == player_weight_min, as.character(player_weight_max), paste(player_weight_min, player_weight_max, sep = "-")),
    date_of_birth = as.Date(date_of_birth, format = "%d/%m/%Y")
  ) |> 
  left_join(
    player_images_metadata, by = "playerId"
  )

player_metadata_sanfl <- sanfl_player_stats |> 
  filter(
    player_id %in% unique(combine_players_both$playerId_sanfl)
  ) |> 
  left_join(sanfl_team_map, by = c("leagueCode" = "comp", "teamCode" = "teamCode"), suffix = c("", "_current")) |>
  transmute(
    playerId = player_id, player_url_sanfl = paste0("https://sanfl.com.au/league/clubs/", club_slug, "/", player_id), 
    player_image_sanfl = if_else(image == "", NA_character_, image)
  ) |> 
  distinct(playerId, .keep_all = TRUE)

system.time({
  player_metadata_wafl <- player_details_wafl |> 
    filter(
      id %in% unique(combine_players_both$playerId_wafl)
    ) |> 
    transmute(
      playerId = id, 
      player_url_wafl = paste0("https://wafl.com.au/player/", slug),
      player_image_wafl = map_chr(slug, get_wafl_player_image)
    )
})

player_metadata_u18_champs <- u18_champs_player_stats_mapped |> 
  filter(
    playerId_combined %in% unique(combine_players_both$playerId),
    !is.na(playerId_combined),
  ) |> 
  select(playerId = playerId_combined, player_url_u18_champs = player_url) |> 
  distinct()

combine_player_details <- combine_players_both |> 
  rename(player_first_name = NAME, player_surname = SURNAME, state = STATE, state_league_club = `STATE LEAGUE CLUB`, community_club = `COMMUNITY CLUB`) |> 
  select(-capitalised_name) |> 
  left_join(player_metadata_afl, by = c("playerId")) |> 
  left_join(player_metadata_sanfl, by = c("playerId_sanfl" = "playerId")) |> 
  left_join(player_metadata_wafl, by = c("playerId_wafl" = "playerId")) |> 
  left_join(player_metadata_u18_champs, by = c("playerId")) |> 
  mutate(
    player_url_afl = if_else(is.na(playerId), NA_character_, paste0("https://www.afl.com.au/draft/prospect/2023?playerId=", playerId)),
    player_urls = pmap(list(player_url_u18_champs, player_url_wafl, player_url_sanfl, player_url_afl), ~ na_rm(c(..1, ..2, ..3, ..4))),
    player_url = map_chr(player_urls, head, n = 1),
    player_images = pmap(list(player_image_wafl, player_image_sanfl, player_images_afl), ~ na_rm(c(..1, ..2, ..3))),
    player_image = map_chr(player_images, ~{
      if(length(.x) == 0L) {
        NA_character_
      } else {
        head(.x, n = 1)
      } 
    })
  ) |> 
  group_by(
    player_first_name, player_surname, gender, state, state_league_club, community_club,national_combine,
    date_of_birth, player_height_min, player_height_max, player_height_range, player_weight_min, player_weight_max, player_weight_range
  ) |> 
  summarise(
    playerIds = c(playerId,  playerId_wafl, playerId_sanfl) |> na_rm() |> list(),
    player_urls = reduce(player_urls, c) |> list(),
    player_url = head(player_url, n = 1),
    player_images = reduce(player_images, c) |> list(),
    player_image = head(player_image, n = 1),
    .groups = "drop"
  ) |> 
  mutate(
    playerId = map_chr(playerIds, head, n = 1),
    season_stats = map(playerIds, ~{
      combine_player_seasons |> 
        filter(playerId %in% .x) |> 
        group_by(tier_short) |>
        summarise(
          fantasy_points = sum(games_played * fantasy_points) / sum(games_played),
          games_played = sum(games_played),
          .groups = "drop"
        ) |> 
        pivot_wider(names_from = tier_short, values_from = c("fantasy_points", "games_played")
        )
      
    })
  ) |> 
  unnest(season_stats) |> 
  relocate(c("games_played_state_underage", "fantasy_points_state_underage", "games_played_interstate_underage", "fantasy_points_interstate_underage",
             "games_played_state_reserves", "fantasy_points_state_reserves", "games_played_state_league", "fantasy_points_state_league"), 
           .after = "player_image") |> 
  left_join(
    phantom_draft_rankings |> select(-player_name_upper) |> rename_with(~paste0("phantom_draft_", .x), -all_of("playerId")), 
    by = "playerId"
  ) #|> 
  # arrange(
  #   phantom_draft_afl, phantom_draft_abc, phantom_draft_sporting_news, phantom_draft_fox_sports, 
  # )
  

write_parquet(combine_player_details, "players/data/processed/combine_player_details.parquet")
write_parquet(combine_player_seasons, "players/data/processed/combine_player_seasons.parquet")
write_parquet(combine_player_stats, "players/data/processed/combine_player_stats.parquet")

