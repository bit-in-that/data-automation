# this script makes an estimation of the autofill picks of each player
library(dplyr)
library(tidyr)
library(arrow)

autofill_date <- as.Date("2024-03-16")

player_selections_long <- read_parquet("afl_fantasy/data/processed/2024/player_selections_long.parquet")
af_player_data <- read_parquet("afl_fantasy/data/processed/2024/af_player_data.parquet") |> 
  select(id, team_name)

autofill_picks <- player_selections_long |> 
  left_join(af_player_data, by = "id") |> 
  mutate(
    before_saturday = team_name %in% c("Richmond", "Carlton", "Sydney Swans", "Collingwood")
  ) |> 
  # player_selections_long |> 
  filter(
    snapshot_date %in% c(autofill_date - 1, autofill_date)
  ) |> 
  # select(id, first_name, last_name, before_saturday, team_name, selections, selections_diff, snapshot_date) |> 
  group_by(id, first_name, last_name, before_saturday, team_name) |> 
  summarise(
    change_previous = head(selections_diff, 1),
    change_post_autofill = tail(selections_diff, 1),
    .groups = "drop"
  ) |>
  mutate(
    increase_decrease_ratio = sum(change_previous * (change_previous > 0)) / sum(-change_previous * (change_previous < 0)),
    total_decreases_post_autofill = sum(-change_post_autofill * (change_post_autofill < 0)),
    bau_increases_post_autofill = total_decreases_post_autofill * increase_decrease_ratio
  ) |>
  # ASSUMPTION: we will only be making adjustments to players who have had an increase (assume anyone who decreased in ownership had negligible auto-fills)
  filter(change_post_autofill > 0) |> 
  mutate(
    total_decreases_autofill = sum(-change_post_autofill * (change_post_autofill < 0)),
    proportion_in_previous = change_previous / sum(change_previous),
    bau_change_post_autofill = round(proportion_in_previous * bau_increases_post_autofill, 0),
    selections_autofill = change_post_autofill - bau_change_post_autofill,
    selections_autofill = if_else(selections_autofill > 0, selections_autofill, 0)
  ) |> 
  select(id, selections_autofill)

autofill_picks |> 
  write_parquet("afl_fantasy/data/processed/2024/autofill_picks.parquet")
