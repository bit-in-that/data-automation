# this script makes an estimation of the autofill picks of each player
library(dplyr)
library(tidyr)
library(arrow)

player_selections_long <- read_parquet("afl_fantasy/data/processed/2024/player_selections_long.parquet")
af_player_data <- read_parquet("afl_fantasy/data/processed/2024/af_player_data.parquet") |> 
  select(id, team_name)


# attach on a column indicating if player played before Saturday
table(af_player_data$team_name)


player_selections_long |> 
  left_join(af_player_data, by = "id") |> 
  mutate(
    before_saturday = team_name %in% c("Richmond", "Carlton", "Sydney Swans", "Collingwood")
  ) |> 
  # player_selections_long |> 
  filter(
    snapshot_date %in% as.Date(c("2024-03-15", "2024-03-16"))
  ) |> 
  # select(id, first_name, last_name, before_saturday, team_name, selections, selections_diff, snapshot_date) |> 
  group_by(id, first_name, last_name, before_saturday, team_name) |> 
  summarise(
    change_previous = head(selections_diff, 1),
    change_post_autofill = tail(selections_diff, 1),
    .groups = "drop"
  ) |>
  mutate(
    increase_decrease_ratio = sum(change_previous * (change_previous > 0)) / sum(-change_previous * (change_previous < 0))
  ) |> 
  # ASSUMPTION: we will only be making adjustments to players who have had an increase (assume anyone who decreased in ownership had negligible auto-fills)
  filter(change_post_autofill > 0) |> 
  mutate(
    total_change_previous = sum(change_previous * !before_saturday * (change_previous > 0) * (change_post_autofill > 0)),
    previous_change_post_saturday = if_else(before_saturday | (change_previous < 0) | (change_post_autofill < 0), 0, change_previous) / total_change_previous * 100
  ) |> 
  View()
  filter(selections > 0) |> 
  pull(selections) |> 
  sum()

