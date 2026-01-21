library(dplyr)
library(tidyr)
library(arrow)
library(digest)

player_selections_initial <- read_parquet("2026/output/player_selections.parquet")

unique_snapshot_times <- player_selections_initial |> 
  group_by(snapshot_time) |> 
  arrange(id) |> 
  summarise(
    hash = digest(ownership, algo = "xxhash64"),
    .groups = "drop"
  ) |> 
  arrange(snapshot_time) |> 
  distinct(hash, .keep_all = TRUE) |> 
  pull(snapshot_time)

player_selections_long <- player_selections_initial |> 
  filter(snapshot_time %in% unique_snapshot_times) |> 
  mutate(
    snapshot_date = as.Date(snapshot_time, tz = "Australia/Sydney") - 1
  ) |> 
  group_by(snapshot_date) |>
  mutate(
    completion_percentage = sum(ownership) / 3000,
  ) |> 
  ungroup() |> 
  mutate(
    ownership_adjusted = round(ownership / completion_percentage, 2)
  ) |>
  group_by(id) |> 
  mutate(
    completion_percentage_diff = c(head(completion_percentage), diff(completion_percentage)),
    ownership_diff = c(head(ownership), diff(ownership)),
    ownership_adjusted_diff = c(head(ownership_adjusted), diff(ownership_adjusted))
  ) |> 
  ungroup() |> 
  mutate(
    text_snapshot = paste0("Snapshot Date: ", format(snapshot_date, format = "%Y-%m-%d"), "<br />Ownership (Adjusted): ", round(ownership_adjusted, 2), "%<br />Raw Ownership (Official): ", round(ownership, 2), "%<br />Team completion: ",round(100*completion_percentage, 1), "%"),
    text_daily_change = paste0("Snapshot Date: ", format(snapshot_date, format = "%Y-%m-%d"), "<br />Change In Adjusted Ownership: ", round(ownership_adjusted_diff, 2), "%<br />Change In Raw Ownership (Official):", ownership_diff, "%<br />Change Team completion: ",round(100*completion_percentage_diff, 1), "%")
  )


write_parquet(player_selections_long, "2026/output/player_selections_long.parquet")
