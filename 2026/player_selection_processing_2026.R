library(dplyr)
library(tidyr)
library(arrow)

# source("_examples/modules/afl_fantasy_apis.R")

bound_values <- function(x, max_value) {
  case_when(
    is.nan(x) ~ 0,
    x>max_value ~ max_value,
    TRUE ~ x
  ) |> 
    round(digits = 2)
}


player_selections_initial <- read_parquet("2026/output/player_selections.parquet")

player_selections_long <- player_selections_initial |> 
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
    completion_percentage_diff = c(0, diff(completion_percentage)),
    ownership_diff = c(0, diff(ownership)),
    ownership_adjusted_diff = c(0, diff(ownership_adjusted))
  ) |> 
  ungroup() |> 
  mutate(
    text_snapshot = paste0("Snapshot Date: ", format(snapshot_date, format = "%Y-%m-%d"), "<br />Ownership (Adjusted): ", round(ownership_adjusted, 2), "%<br />Raw Ownership (Official): ", round(ownership, 2), "%<br />Team completion: ",round(100*completion_percentage, 1), "%"),
    text_daily_change = paste0("Snapshot Date: ", format(snapshot_date, format = "%Y-%m-%d"), "<br />Daily Movement In Adjusted Ownership: ", round(ownership_adjusted_diff, 2), "<br />Daily Movement In Raw Ownership  (Official):", ownership_diff, "%<br />Team completion: ",round(100*completion_percentage_diff, 1), "%")
  )


write_parquet(player_selections_long, "2026/output/player_selections_long.parquet")
