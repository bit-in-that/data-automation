library(httr2)
library(purrr)
library(dplyr)
library(arrow)

source("afl_fantasy/modules/get_af_session_id.R")
session_id <- get_af_session_id()

overall_ranks_2023 <- read_parquet("afl_fantasy/data/processed/overall_ranks_2023.parquet") |> 
  select(user_id, rank_2023 = rank)

overall_ranks_2024 <- read_parquet("afl_fantasy/data/raw/2024/afl_fantasy_rankings.parquet") |> 
  select(user_id, team_id, rank_2024 = rank)

overall_ranks <- overall_ranks_2024 |> 
  left_join(overall_ranks_2023, by = "user_id") |> 
  filter(rank_2024 <= 10000L | rank_2023 <= 10000L)

convert_lineup_list_to_df <- function(lineup_list, round_num = NULL) {
    team_name <- lineup_list$name
    user_id <- lineup_list$user_id
    team_id <- lineup_list$id
    if(is.null(round_num)) {
      round <- lineup_list$scoreflow |> names() |> as.integer() |> max()
    } else {
      round <- round_num
    }
    if(!is.null(lineup_list$start_round)) {
      if(round < lineup_list$start_round) {
        return(NULL)
      }
    }
    
    round <- as.character(round)
    
    score = lineup_list$scoreflow[[round]]
    
    captain <- lineup_list$lineup$captain
    vice_captain <- lineup_list$lineup$vice_captain
    
    if(is.null(lineup_list$firstname)) {
      first_name <- NA_character_
      last_name <- NA_character_
    } else {
      first_name <- lineup_list$firstname 
      last_name <- lineup_list$lastname 
    }

  on_field_players <- lineup_list$lineup[1:4] |>
    imap(~{
      tibble(
        player_id = as.integer(.x),
        positon = .y
      )
    }) |> 
    bind_rows() |> 
    mutate(
      is_bench = FALSE
    )
  
  bench_players <- lineup_list$lineup$bench[1:4] |> 
    imap(~{
      tibble(
        player_id = as.integer(.x),
        positon = .y
      )
    }) |> 
    bind_rows() |> 
    mutate(
      is_bench = TRUE
    )
  
  emergencies <- as.integer(lineup_list$lineup$bench$emergency)
  
  on_field_players |> 
    bind_rows(
      bench_players
    ) |> 
    mutate(
      team_name = team_name,
      first_name = first_name,
      last_name = last_name,
      user_id = user_id,
      team_id = team_id,
      round = round,
      score = score,
      value = lineup_list$value,
      salary_cap = lineup_list$salary_cap,
      salary_remaining = salary_cap - value,
      formation = lineup_list$formation,
      activated_at = lineup_list$activated_at,
      valid = lineup_list$valid,
      complete_first_time = lineup_list$complete_first_time#,
      # snapshot_time = lineup_list$snapshotTime
    ) |> 
    relocate(c("player_id", "positon", "is_bench"), .after = "complete_first_time") |> 
    mutate(
      is_captain = player_id %in% captain,
      is_vice_captain = player_id %in% vice_captain,
      is_emergency = player_id %in% emergencies
    )
}

handle_data <- function(resp) {
  resp_json <- resp |> 
    resp_body_json()
  
  if(resp_json$success == 1) {
    convert_lineup_list_to_df(resp_json$result)
  } else {
    NULL
  }
}

params <- list(round_num = NULL)
headers <- list(cookie = paste0("session=", session_id))

base_req <- request("https://fantasy.afl.com.au/afl_classic/api/teams_classic/show") |> 
  req_url_query(!!!params) |>
  req_headers(!!!headers)


req_list <- overall_ranks |>
  pull(team_id) |> 
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
