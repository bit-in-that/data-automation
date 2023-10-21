library(dplyr)
library(arrow)

sanfl_player_stats <- read_parquet("state_leagues/data/raw/sanfl_player_stats.parquet")
wafl_player_stats <- read_parquet("state_leagues/data/raw/wafl_player_stats.parquet")
wafl_id_conversion_table <- read_parquet("state_leagues/data/processed/wafl_id_conversion_table.parquet")

# TODO: get the player stats for U18

combine_data_ids |> 
  left_join(
    wafl_id_conversion_table,
    by = c("playerId" = "official_id")
  ) |> View()
