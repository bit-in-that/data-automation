library(httr2)
library(dplyr)
library(arrow)
library(purrr)
library(stringr)
library(jsonlite)

handle_data <- function(resp) {
  if(resp$headers$`Content-Type` == "application/json") {
    parsed_data <- resp |> 
      resp_body_json()
  } else {
    parsed_data <- resp |> 
      resp_body_string() |> 
      parse_json()
  }

  match_info <- parsed_data$report$matchInfo
  
  player_info <- parsed_data$report$players |> 
    bind_rows()
  
  action_ids <- seq_along(parsed_data$report$matchTrxs)
  
  parsed_data$report$matchTrxs |> 
    map2(action_ids, ~c(list(action_number = .y), .x)) |> 
    bind_rows() |> 
    mutate(stats = as.character(stats)) |> 
    select(-squadId) |> 
    left_join(bind_cols(player_info, match_info), by = "playerId")
  
}

source("players/modules/get_afl_token.R") #requires httr
afl_token <- get_afl_token()

match_metadata_afl <- read_parquet("metadata/data/processed/match_metadata_afl.parquet")
round_metadata_afl <- read_parquet("metadata/data/processed/round_metadata_afl.parquet")

match_data <- match_metadata_afl |> 
  filter(competition_id == 1L, year >= 2017) |> 
  left_join(
    select(round_metadata_afl, round_id = id, round_name = name),
    by = "round_id"
  ) |> 
  transmute(match_id = str_remove(providerId, "^CD_M"), year, round_name, roundNumber)


headers <- list('x-media-mis-token' = afl_token)

base_req <- request("https://www.afl.com.au/statspro/json") |> 
  req_headers(!!!headers)

match_ids <- match_data$match_id

req_list <- match_ids |> 
  map(~base_req |> req_url_path_append(paste0("vision-trxs-", .x, ".json")))

system.time({
  resp_list <- req_list |> 
    req_perform_parallel(on_error = "continue")
})

system.time({
  resp_tbl <- resp_list |> 
    resps_successes() |>
    resps_data(handle_data)
})

# match_ids[808]
# resp_list[808]
is_valid_output <- (resp_list |> map_lgl(is, class2 = "httr2_response")) |> which()

player_actions <- resp_tbl |> 
  distinct(matchId) |> 
  transmute(
    matchId,
    match_id = match_ids[is_valid_output]
  ) |> 
  right_join(
    resp_tbl, by = "matchId"
  ) |> 
  select(-matchId)


player_actions |> 
  write_parquet("players/data/raw/player_actions.parquet")
