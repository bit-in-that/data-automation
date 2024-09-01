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


player_selections_initial <- read_parquet("afl_fantasy/data/raw/2024/player_selections.parquet")
autofill_picks <- read_parquet("afl_fantasy/data/processed/2024/autofill_picks.parquet")

player_selections_long <- player_selections_initial |> 
  group_by(snapshot_time) |> 
  filter(selections == max(selections)) |> 
  ungroup() |> 
  distinct(.keep_all = TRUE) |> 
  mutate(
    fantasy_coaches = round(selections / owned_by * 100, 0)
  ) |> 
  # filter out the early small sample size day because its pretty meaningless
  filter(fantasy_coaches > 1000) |> 
  select(fantasy_coaches, snapshot_time) |> 
  distinct(fantasy_coaches, .keep_all = TRUE) |> 
  inner_join(player_selections_initial, y = _, by = "snapshot_time") |> 
  mutate(
    snapshot_date = as.Date(snapshot_time, tz = "Australia/Sydney") - 1
  ) |> 
  left_join(autofill_picks, by = "id") |> 
  mutate(
    selections_autofill = coalesce(selections_autofill, 0L),
    # overwrite the selections numbers with ones that are adjusted for autofills for all dates after the snapshot data
    selections_no_autofill = if_else(snapshot_date >= autofill_date, selections - selections_autofill ,selections),
    owned_by_no_autofill = round(selections_no_autofill / fantasy_coaches, 4)*100
  ) |> 
  group_by(snapshot_date, fantasy_coaches) |>
  mutate(
    completed_teams = sum(selections) / 30,
    completed_teams_no_autofill = sum(selections_no_autofill) / 30,
    completion_percentage = completed_teams / fantasy_coaches,
    completion_percentage_no_autofill = completed_teams_no_autofill / fantasy_coaches,
    completion_percentage_captain = sum(selections_captain) / 100,
    completion_percentage_vice_captain = sum(selections_vice_captain) / 100,
    completion_percentage_bench = sum(selections_bench) / 8 / 100,
    completion_percentage_emergency = sum(selections_emergency) / 4 / 100
  ) |> 
  ungroup() |> 
  mutate(
    owned_by_adjusted = round(owned_by_no_autofill / completion_percentage_no_autofill, 2)
  ) |>
  group_by(id) |> 
  mutate(
    selections_diff = c(head(selections, 1), diff(selections)),
    fantasy_coaches_diff = c(head(fantasy_coaches, 1), diff(fantasy_coaches)),
    owned_by_adjusted_diff = c(head(owned_by_adjusted, 1), diff(owned_by_adjusted))
  ) |> 
  ungroup() |> 
  mutate(
    selections_adjusted_diff = round(owned_by_adjusted_diff * completed_teams / 100, 0),
    # snapshot_date = format(snapshot_time, format = "%Y_%m_%d"),
    text_snapshot = paste0("Snapshot Date: ", format(snapshot_date, format = "%Y-%m-%d"), "<br />Owned By (Adjusted): ", round(owned_by_adjusted, 2), "%<br />Owned By (Official): ", round(owned_by, 2), "% (", selections,")<br /># Coaches: ", fantasy_coaches, " (", round(100*completion_percentage, 1), "% complete)"),
    text_daily_change = paste0("Snapshot Date: ", format(snapshot_date, format = "%Y-%m-%d"), "<br />Daily Movement In Adjusted Ownership: ", round(owned_by_adjusted_diff, 2), "% (", selections_adjusted_diff,")", "<br /># Coaches: ", fantasy_coaches, " (", round(100*completion_percentage, 1), "% complete)"),
    selections_captain_adjusted = bound_values(selections_captain / completion_percentage_captain, 100),
    selections_captain_if_owned = bound_values(selections_captain / owned_by * 100, 100),
    selections_vice_captain_adjusted = bound_values(selections_vice_captain / completion_percentage_vice_captain, 100),
    selections_vice_captain_if_owned = bound_values(selections_vice_captain / owned_by  * 100, 100),
    selections_bench_adjusted = bound_values(selections_bench / completion_percentage_bench, 100),
    selections_bench_if_owned = bound_values(selections_bench / owned_by * 100, 100),
    selections_emergency_adjusted = bound_values(selections_emergency / completion_percentage_emergency, 100),
    selections_emergency_if_owned = bound_values(selections_emergency / owned_by * 100, 100),
    selections_emergency_if_bench = bound_values(selections_emergency / selections_bench * 100, 100),
    selections_any_captain_adjusted = selections_captain_adjusted + selections_vice_captain_adjusted,
    selections_any_captain_if_owned = selections_captain_if_owned + selections_vice_captain_if_owned
  )

# Note currently used but might come in handy later:
if(FALSE) {
  player_selections_wide <- player_selections_long |> 
    mutate(
      snapshot_date = format(snapshot_time, format = "%Y_%m_%d")
    ) |> 
    pivot_wider(id_cols = c("id", "first_name", "last_name"), names_from = "snapshot_date", 
                values_from = c("selections", "fantasy_coaches", "owned_by_adjusted"))
  
}

write_parquet(player_selections_long, "afl_fantasy/data/processed/2024/player_selections_long.parquet")
