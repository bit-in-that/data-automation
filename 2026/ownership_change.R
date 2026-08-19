# # Closest snapshot for the article determined to be 9AM 29 January:
# player_selections |>
#   select(id, first_name, last_name, price, position, current_ownership = ownership) |>
#   right_join(player_selections_initial, by = "id") |>
#   filter(snapshot_time < as.POSIXct("2026-01-29 9:01") & snapshot_time > as.POSIXct("2026-01-29 9:00")) |>
#   transmute(
#     id,
#     player_name = paste(first_name, last_name),
#     price,
#     position,
#     ownership,
#     current_ownership
#   ) |>
#   clipr::write_clip()
# TODO: pre and post origin snapshots



library(lubridate)
library(dplyr)
library(purrr)
library(arrow)
library(httr2)
library(fitzRoy)
library(stringr)

player_details_afl <- fetch_player_details_afl(season = 2026)


player_teams <- player_details_afl |>
  transmute(
    id = providerId |>
      str_remove("^CD_I") |>
      as.integer(),
    team
  )

player_selections_initial <- read_parquet("2026/output/player_selections.parquet")


players_url <- "https://fantasy.afl.com.au/json/fantasy/players.json"
players_coach_url <- "https://fantasy.afl.com.au/json/fantasy/coach/players.json"

players <- request(players_url) |>
  req_perform() |>
  resp_body_json()

players_coach <- request(players_coach_url) |>
  req_perform() |>
  resp_body_json()

player_data_latest <- players |>
  map(~{
    tibble(
      id = .x$id,
      player_name = paste(.x$firstName, .x$lastName),
      price = .x$price,
      position = paste(unlist(.x$position), collapse = "/"),
      ownership_current = .x$ownership[["2"]],
    )
  }) |>
  list_rbind() |>
  left_join(player_teams, by = "id") |>
  relocate(team, .before = ownership_current)



target_time <- hms::hms(hours = 22, minutes = 0, seconds = 0)


selected_snapshots <- player_selections_initial |>
  distinct(snapshot_time) |>
  mutate(
    date = as_date(snapshot_time),
    time = hms::as_hms(snapshot_time),
    time_diff = abs(as.numeric(time - target_time))
  ) |>
  group_by(date) |>
  slice_min(time_diff, n = 1) %>%
  ungroup() |>
  pull(snapshot_time)


player_data_latest |>
  left_join(
    player_selections_initial |>
      filter(snapshot_time %in% selected_snapshots),
    by = "id"
    ) |>
  mutate(
    snapshot_date = as_date(snapshot_time),
    lookup = paste0(player_name, snapshot_date)
    ) |>
  clipr::write_clip()


player_data_latest |>
  # select(id, player_name, price, position, team, current_ownership) |>
  left_join(
    player_selections_initial |>
      filter(snapshot_time < as.POSIXct("2026-02-15 23:00") & snapshot_time > as.POSIXct("2026-02-15 22:10")),
    by = "id") |>
  # filter(snapshot_time < as.POSIXct("2026-01-29 9:01") & snapshot_time > as.POSIXct("2026-01-29 9:00")) |>
  select(
    id,
    player_name,
    price,
    position,
    team,
    ownership_original = ownership,
    ownership_current
  ) |>
  clipr::write_clip()

