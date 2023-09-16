library(arrow)
library(dplyr)
library(stringr)

player_details_all <- read_parquet("players/data/raw/player_details_all.parquet")
player_details_wafl <- read_parquet("state_leagues//data/raw/player_details_wafl.parquet")


# manual fixes to players:
standardise_name <- function(x) {
  str_remove_all(str_remove(str_to_upper(x), "( \\(JNR\\)| JN?R)$"), "-| ")
}



wa_player_official_ids <- player_details_all |> 
  mutate(
    # Lachlan Godden Fix issue
    playerId = if_else(playerId == "CD_I1013087" & playerDetails.dateOfBirth == "26/09/2005", "CD_I1032889", playerId, playerId),
    # Lachlan Riley Fix
    playerId = if_else(playerId == "CD_I1015872" & playerDetails.dateOfBirth == "02/07/2000", "CD_I1006156", playerId, playerId),
  ) |> 
  select(playerId, playerDetails.givenName, playerDetails.surname, year, competition_name_lookup, 
         playerDetails.recruitedFrom, playerDetails.dateOfBirth, team.teamName) |> 
  mutate(
    is_wa = if_else(str_detect(playerDetails.recruitedFrom, "\\(WA|Fremantle|West Coast"), TRUE, FALSE, 
                    competition_name_lookup == "WAFL" | team.teamName == "West Coast Eagles" | team.teamName == "Fremantle")
    ) |> 
  filter(is_wa) |>
  group_by(playerId) |>
  filter(year == max(year)) |>
  ungroup() |> 
  distinct(playerId, .keep_all = TRUE) |> 
  transmute(
    first = standardise_name(playerDetails.givenName), 
    last = standardise_name(playerDetails.surname), 
    date_of_birth = as.Date(playerDetails.dateOfBirth, "%d/%m/%Y"),
    official_id = playerId
    )

# get use lastname, birthday without duplicates to allow for non-matching first names but remove dups to avoid twins
wa_player_official_ids_alt <- wa_player_official_ids |> 
  group_by(last, date_of_birth) |> 
  filter(n() < 2) |> 
  ungroup() |> 
  select(official_id_alt = official_id, last, date_of_birth)

wafl_id_conversion_table <- player_details_wafl |> 
  select(id, first, last, from_club, club_name, date_of_birth, debut, slug) |> 
  mutate(
    debut = as.integer(if_else(debut == "0", NA, debut)),
    date_of_birth = as.Date(date_of_birth, "%Y-%m-%d"),
    first = standardise_name(first),
    last = standardise_name(last)
  ) |> 
  left_join(
    wa_player_official_ids,
    by = c("first", "last", "date_of_birth")
  ) |>
  left_join(
    wa_player_official_ids_alt,
    by = c("last", "date_of_birth")
  ) |> 
  mutate(official_id = coalesce(official_id, official_id_alt)) |> 
  filter(!is.na(official_id)) |> 
  transmute(
    official_id,
    wafl_id = id,
    wafl_url = paste0("https://wafl.com.au/player/", slug)
  )

# note that some players have multiple WAFL IDs (due to errors) thus the mapping is one AFLID to multiple WAFL IDs
write_parquet(wafl_id_conversion_table, "state_leagues/data/processed/wafl_id_conversion_table.parquet")

