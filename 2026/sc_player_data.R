# try to get snapshots of player selections with time stamps
library(dplyr)
library(purrr)
library(arrow)
library(httr2)

players_url <- players_url <- "https://www.supercoach.com.au/2026/api/afl/classic/v1/players-cf?year=2026&round=1&embed=player_stats,positions"

players <- request(players_url) |> 
  req_perform() |> 
  resp_body_json()

# TDOO: make this a lot nicer
player_data <- players |> 
  map(~{
    tibble(
      id = .x$id,
      feed_id = .x$feed_id,
      first_name = .x$first_name,
      last_name = .x$last_name,
      price = .x$player_stats[[1]]$price,
      position = .x$positions |> map_chr(pluck, "position") |> paste(collapse = "/"),
    )
  }) |> 
  list_rbind()

player_data |> 
  write_parquet("2026/output/sc_player_data.parquet")
