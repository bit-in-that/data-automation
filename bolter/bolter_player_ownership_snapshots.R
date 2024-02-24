library(arrow)
library(dplyr)

source("bolter/modules/get_bolter_session_id.R")
source("bolter/modules/get_bolter_data.R")
bolter_session_id <- get_bolter_session_id()
r0_teams <- c("Melbourne", "Sydney", "Richmond", "GWS Giants", "Collingwood", "Brisbane Lions", "Carlton", "Gold Coast Suns")

bolter_player_ownership_snapshots_initial <- read_parquet("bolter/data/raw/bolter_player_ownership_snapshots.parquet")

bolter_player_ownership_snapshots <- get_bolter_player_data(bolter_session_id) |> 
  transmute(
    player_id, 
    player_name = name,
    team_name = teamName,
    position,
    position2,
    ownership = coalesce(as.numeric(ownership), 0),
    plays_r0 = team_name %in% r0_teams,
    snapshot_time = Sys.time()
  )

bolter_player_ownership_snapshots_initial |>
  bind_rows(bolter_player_ownership_snapshots) |>
  write_parquet("bolter/data/raw/bolter_player_ownership_snapshots.parquet")
