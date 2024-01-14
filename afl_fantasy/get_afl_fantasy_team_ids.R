library(httr2)
library(purrr)
library(tibble)
library(arrow)

source("afl_fantasy/modules/get_af_session_id.R")
session_id <- get_af_session_id()

afl_fantasy_team_ids <- read_parquet("afl_fantasy/data/raw/2024/afl_fantasy_team_ids.parquet")

next_id <- max(afl_fantasy_team_ids$team_id) + 1

handle_data <- function(resp) {
  resp_json <- resp |> 
    resp_body_json()
  
  if(resp_json$success == 1) {
    with(resp_json$result, {
      tibble(
        team_name = name,
        team_id = id,
        user_id = user_id,
        firstname = firstname,
        lastname = lastname
      )
    })
    
  } else {
    NULL
    
  }
}


params <- list(round_num = NULL)
headers <- list(cookie = paste0("session=", session_id))

base_req <- request("https://fantasy.afl.com.au/afl_classic/api/teams_classic/show") |> 
  req_url_query(!!!params) |>
  req_headers(!!!headers)

req_list <- ((1:2000) + next_id) |> 
  map(~base_req |> req_url_query(id = .x))

system.time({
  resp_list <- req_list |> 
    req_perform_parallel(on_error = "return")
  
})

system.time({
  resp_tbl <- resp_list |> 
    resps_successes() |>
    resps_data(handle_data)
})

print(paste("Number of rows added:", nrow(resp_tbl)))

afl_fantasy_team_ids |> 
  bind_rows(resp_tbl) |> 
  write_parquet("afl_fantasy/data/raw/2024/afl_fantasy_team_ids.parquet")
