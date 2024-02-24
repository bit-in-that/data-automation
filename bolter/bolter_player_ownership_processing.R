library(arrow)
library(dplyr)

bolter_player_ownership_snapshots <- read_parquet("bolter/data/raw/bolter_player_ownership_snapshots.parquet")

bolter_player_ownership_processed <- bolter_player_ownership_snapshots |> 
  mutate(snapshot_date = as.Date(snapshot_time, tz = "Australia/Perth") - 1) |> 
  select(-snapshot_time) |> 
  distinct(.keep_all = TRUE) |> 
  group_by(snapshot_date) |> 
  mutate(
    adjustment_factor = sum(ownership * plays_r0) / 30 / 100
  ) |> 
  ungroup() |> 
  mutate(
    ownership_adjusted = if_else(plays_r0, round(ownership / adjustment_factor, 2), 0)
  )

bolter_player_ownership_processed |> 
  write_parquet("bolter/data/processed/bolter_player_ownership_processed.parquet")
