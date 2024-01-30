library(httr2)
library(dplyr)
library(arrow)
library(purrr)
library(tidyr)

handle_data <- function(resp) {
  resp_json <- resp |> 
    resp_body_json()
  
  if(identical(resp_json$matchChains, list())) {
    return(NULL)
  }
  
  resp_json$matchChains |> 
    bind_rows() |> 
    mutate(
      chain_number = seq_along(stats),
      stats = map(stats, bind_rows)
    ) |> 
    unnest(stats, names_sep = "_") |> 
    relocate(chain_number) |> 
    mutate(
      matchId = resp_json$matchId,
      venueWidth = resp_json$venueWidth,
      venueLength = resp_json$venueLength,
      homeTeamId = resp_json$homeTeamId,
      awayTeamId = resp_json$awayTeamId,
      homeTeamDirectionQtr1 = resp_json$homeTeamDirectionQtr1
    )
  
}

source("players/modules/get_afl_token.R") #requires httr
afl_token <- get_afl_token()

match_metadata_afl <- read_parquet("metadata/data/processed/match_metadata_afl.parquet")
round_metadata_afl <- read_parquet("metadata/data/processed/round_metadata_afl.parquet")

match_data <- match_metadata_afl |> 
  filter(competition_id == 1L, year >= 2021) |> 
  left_join(
    select(round_metadata_afl, round_id = id, round_name = name),
    by = "round_id"
  ) |> 
  select(match_id = providerId, year, round_name, roundNumber)


headers <- list('x-media-mis-token' = afl_token)

base_req <- request("https://sapi.afl.com.au/afl/matchPlays") |> 
  req_headers(!!!headers)

req_list <- (match_data$match_id) |> 
  map(~base_req |> req_url_path_append(.x))


system.time({
resp_list <- req_list |> 
  req_perform_parallel(on_error = "continue")
})

system.time({
  resp_tbl <- resp_list |> 
    resps_successes() |>
    resps_data(handle_data)
})

resp_tbl |> 
  write_parquet("players/data/raw/possesion_chains.parquet")
