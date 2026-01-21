# try to get snapshots of player selections with time stamps
library(dplyr)
library(purrr)
library(arrow)
library(httr2)


player_selections_initial <- read_parquet("2026/output/player_selections.parquet")

players_url <- "https://fantasy.afl.com.au/json/fantasy/players.json"
players_coach_url <- "https://fantasy.afl.com.au/json/fantasy/coach/players.json"

players <- request(players_url) |> 
  req_perform() |> 
  resp_body_json()

players_coach <- request(players_coach_url) |> 
  req_perform() |> 
  resp_body_json()

player_selections <- players |> 
  map(~{
    tibble(
      id = .x$id,
      first_name = .x$firstName,
      last_name = .x$lastName,
      dob = .x$dob,
      price = .x$price,
      position = paste(unlist(.x$position), collapse = "/"),
      # seasons = paste(unlist(.x$seasons), collapse = ","),
      # selections = .x$stats$selections,
      # Not sure what this "2" is, need to look into it (are they doing ownership over time?)
      ownership = .x$ownership[["2"]],
      # owned_by = .x$stats$owned_by,
      # selections_captain = .x$stats$selections_info$c,
      # selections_vice_captain = .x$stats$selections_info$vc,
      # selections_bench = .x$stats$selections_info$bc,
      # selections_emergency = .x$stats$selections_info$emg,
    )
  }) |> 
  bind_rows() |> 
  mutate(
    snapshot_time = Sys.time()
  ) |> left_join(
    by = "id",
  players_coach |> 
    map(~{
      tibble(
        id = .x$id,
        picked_bench = .x$pickedBench[["2"]],
        picked_field = .x$pickedField[["2"]]
      )
    }) |> 
    bind_rows()
  )




player_selections_minimal <- player_selections |> 
  select(id, ownership, picked_field, picked_bench, snapshot_time)

player_selections_initial |> 
  bind_rows(player_selections_minimal) |> 
  write_parquet("2026/output/player_selections.parquet")

if(FALSE) { # As to not be run by action:
  player_selections |> 
    mutate(
      ownership_adjusted = round(ownership / sum(ownership) * 3000, 2)
    ) |> 
    clipr::write_clip()
}

