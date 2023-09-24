library(arrow)
library(dplyr)
library(tidyr)

source("aflw_fantasy/modules/get_player_data_afw.R")

top_10000_lineups <- read_parquet("https://github.com/bit-in-that/data-automation/raw/main/aflw_fantasy/data/raw/top_10000_lineups.parquet")

player_data_r1 <- get_player_data(by_round = TRUE) |> 
  filter(round == 1)  |> 
  select(player_id = id, player_position = position, current_price = cost, avgPoints, starting_price = round_price)

top_10000_worst_players <- top_10000_lineups |> 
  left_join(player_data_r1, by = "player_id") |> 
  group_by(
    user_id, player_position
  ) |> 
  mutate(
    cost_rank = as.integer(rank(-player_cost, ties.method = "first")),
    position_count = if_else(player_position == "RUC", 1L, 5L)
  ) |>
  filter(cost_rank == position_count)

top_10000_worst_players |> 
  group_by(player_id, player_name, player_position, current_price, starting_price, avgPoints) |> 
  summarise(
    first = sum(overall_rank == 1),
    top_100 = sum(overall_rank <= 100)/100,
    top_1000 = sum(overall_rank <= 1000)/1000,
    top_2000 = sum(overall_rank <= 2000)/2000,
    top_5000 = sum(overall_rank <= 5000)/5000,
    top_10000 = sum(overall_rank <= 10000)/10000
  ) |> View()

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
