library(httr2)
library(purrr)
library(dplyr)
library(arrow)

source("afl_fantasy/modules/get_af_session_id.R")
session_id <- get_af_session_id()

afl_fantasy_team_ids <- read_parquet("afl_fantasy/data/raw/2024/afl_fantasy_team_ids.parquet") |> 
  filter(team_id <= 150100L) # this is the team ID where round 1 playing teams stop (hard to make up 1800ish points if you start later)

handle_data <- function(resp) {
  resp_json <- resp |> 
    resp_body_json()
  
  if(resp_json$success == 1) {
    with(resp_json$result, {
      tibble(
        team_id = id,
        points = points,
        rank = rank
      )
    })
    
  } else {
    NULL
    
  }
}

params <- list(round_num = NULL)
headers <- list(cookie = paste0("session=", session_id))

base_req <- request("https://fantasy.afl.com.au/afl_classic/api/teams_classic/snapshot") |> 
  req_url_query(!!!params) |>
  req_headers(!!!headers)

req_list <- afl_fantasy_team_ids |> 
  pull(user_id) |> 
  map(~base_req |> req_url_query(user_id = .x))

system.time({
  resp_list <- req_list |> 
    req_perform_parallel(on_error = "return")
  
})

system.time({
  resp_tbl <- resp_list |> 
    resps_successes() |>
    resps_data(handle_data)
})

resp_tbl |> 
  left_join(
    afl_fantasy_team_ids, by = "team_id" 
  ) |> 
  relocate(points, rank, .after = "lastname") |> 
  write_parquet("afl_fantasy/data/raw/2024/afl_fantasy_rankings.parquet")
