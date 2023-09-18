library(arrow)
library(dplyr)
library(tidyr)

source("aflw_fantasy/modules/get_player_data_afw.R")

top_10000_lineups <- read_parquet("aflw_fantasy/data/raw/top_10000_lineups.parquet")
top_10000_lineups <- read_parquet("https://github.com/bit-in-that/data-automation/raw/main/aflw_fantasy/data/raw/top_10000_lineups.parquet")
player_data <- get_player_data() |> 
  select(player_id = id, player_position = position)

top_10000_worst_players <- top_10000_lineups |> 
  left_join(player_data, by = "player_id") |> 
  group_by(
    user_id, player_position
  ) |> 
  mutate(
    cost_rank = as.integer(rank(-player_cost, ties.method = "first")),
    position_count = if_else(player_position == "RUC", 1L, 5L)
  ) |>
  filter(cost_rank == position_count)

top_10000_worst_players |> 
  pivot_wider(id_cols = "user_id", names_from = "player_position", values_from = "player_name") |> View()

top_10000_worst_players |> 
  pivot_wider(id_cols = "user_id", names_from = "player_position", values_from = "player_cost") |> 
  group_by(FWD, DEF, MID, RUC) |> View()
  summarise(
    count = n(),
    .groups = "drop"
  ) |> 
  mutate(
    ALL_min = pmin(FWD, DEF, MID, RUC),
    ALL_sum = FWD + DEF + MID + RUC
  ) |> 
  View()
