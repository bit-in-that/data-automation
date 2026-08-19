# try to get snapshots of player selections with time stamps
library(dplyr)
library(purrr)
library(arrow)

source("aflw_fantasy/modules/get_player_data_afw.R")

player_selections_initial <- read_parquet("aflw_fantasy/data/raw/2024/player_selections.parquet")
players <- get_player_data()

player_selections <- players |> 
    select(
      id = id,
      first_name = firstName,
      last_name = lastName,
      selections,
      # owned_by = .x$stats$owned_by,
      # selections_captain = .x$stats$selections_info$c,
      # selections_vice_captain = .x$stats$selections_info$vc,
      # selections_bench = .x$stats$selections_info$bc,
      # selections_emergency = .x$stats$selections_info$emg,
    ) |> 
  mutate(
    snapshot_time = Sys.time()
  )
  
player_selections_initial |>
  bind_rows(player_selections) |>
  write_parquet("aflw_fantasy/data/raw/2024/player_selections.parquet")

