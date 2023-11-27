library(rvest)
library(dplyr)

player_position_url <- "https://www.afl.com.au/news/1066775/fantasy-2024-every-player-every-position"

player_positions <- player_position_url |> 
  read_html() |> 
  html_table(header = TRUE) |> 
  bind_rows() |> 
  distinct()


write_parquet(player_positions, "afl_fantasy/data/raw/player_positions.parquet")
