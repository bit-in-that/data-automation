# try to get snapshots of player selections with time stamps
library(dplyr)
library(purrr)
library(arrow)
library(httr2)

players_url <- "https://fantasy.afl.com.au/json/fantasy/players.json"

players <- request(players_url) |> 
  req_perform() |> 
  resp_body_json()

# TDOO: make this a lot nicer
player_data <- players |> 
  map(~{
    tibble(
      id = .x$id,
      first_name = .x$firstName,
      last_name = .x$lastName,
      dob = .x$dob,
      price = .x$price,
      position = paste(unlist(.x$position), collapse = "/"),
    )
  }) |> 
  list_rbind()

player_data |> 
  write_parquet("2026/output/player_data.parquet")
