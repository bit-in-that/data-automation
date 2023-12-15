# try to get snapshots of player selections with time stamps
library(dplyr)
library(purrr)
library(arrow)

source("_examples/modules/afl_fantasy_apis.R")

player_selections_initial <- read_parquet("afl_fantasy/data/raw/2024/player_selections.parquet")

players <- get_afl_fantasy_players()

player_selections <- players |> 
  map(~{
    tibble(
      id = .x$id,
      first_name = .x$first_name,
      last_name = .x$last_name,
      selections = .x$stats$selections,
      owned_by = .x$stats$owned_by,
      selections_captain = .x$stats$selections_info$c,
      selections_vice_captain = .x$stats$selections_info$vc,
      selections_bench = .x$stats$selections_info$bc,
      selections_emergency = .x$stats$selections_info$emg,
    )
  }) |> 
  bind_rows() |> 
  mutate(
    snapshot_time = Sys.time()
  )
  
player_selections_initial |> 
  bind_rows(player_selections) |> 
  write_parquet("afl_fantasy/data/raw/2024/player_selections.parquet")

