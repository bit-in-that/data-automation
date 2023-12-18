library(dplyr)
library(tidyr)
library(arrow)

source("_examples/modules/afl_fantasy_apis.R")

player_selections_initial <- read_parquet("afl_fantasy/data/raw/2024/player_selections.parquet")

player_selections_long <- player_selections_initial |> 
  group_by(snapshot_time) |> 
  filter(selections == max(selections)) |> 
  ungroup() |> 
  distinct(.keep_all = TRUE) |> 
  mutate(
    fantasy_coaches = round(selections / owned_by * 100, 0)
  ) |> 
  select(fantasy_coaches, snapshot_time) |> 
  distinct(fantasy_coaches, .keep_all = TRUE) |> 
  inner_join(player_selections_initial, y = _, by = "snapshot_time") |> 
  group_by(snapshot_time, fantasy_coaches) |>
  mutate(
    completed_teams = sum(selections) / 30,
    completion_percentage = completed_teams / fantasy_coaches
  ) |> 
  ungroup() |> 
  mutate(
    owned_by_adjusted = round(owned_by / completion_percentage, 2)
  ) |> 
  group_by(id) |> 
  mutate(
    selections_diff = c(0L, diff(selections)),
    fantasy_coaches_diff = c(0L, diff(fantasy_coaches)),
    owned_by_adjusted_diff = c(0L, diff(owned_by_adjusted))
  ) |> 
  ungroup() |> 
  mutate(
    snapshot_date = format(snapshot_time, format = "%Y_%m_%d")
  )


player_selections_wide <- player_selections_long |> 
  mutate(
    snapshot_date = format(snapshot_time, format = "%Y_%m_%d")
  ) |> 
  pivot_wider(id_cols = c("id", "first_name", "last_name"), names_from = "snapshot_date", 
              values_from = c("selections", "fantasy_coaches", "owned_by_adjusted"))

write_parquet(player_selections_long, "afl_fantasy/data/processed/2024/player_selections_long.parquet")
