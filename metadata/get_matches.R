library(httr)
library(dplyr)
library(arrow)
library(purrr)
library(tidyr)

round_metadata_afl <- read_parquet("metadata/data/processed/round_metadata_afl.parquet")

get_match_id_map <- function() {
  response <- GET("https://aflapi.afl.com.au/afl/v2/matches/idmap")
  list_map <- content(response)$idMapResponse$ids
  
  tibble(
    provider_id = names(list_map),
    id = unlist(list_map)
  )
  
}

get_match_info <- function(roundNumber, competitionId, compSeasonId, pageSize = 100, convert_to_df = TRUE, unnest_columns = TRUE) {
  
  query <- list(
    competitionId = competitionId, 
    compSeasonId = compSeasonId,
    pageSize = pageSize,
    roundNumber = roundNumber
  )
  
  response <- GET("https://aflapi.afl.com.au/afl/v2/matches", query = query)
  output <- content(response)
  
  if(convert_to_df) {
    output <- output$matches |> 
      map(~ {
        imap(.x, \(column_value, column_name) {
          if(!is.list(column_value)) {
            return(column_value)
          } else if(length(column_value) == 0) {
            return(list(NULL))
          }
          column_value |>
            list_flatten() |> list_flatten() |>
            as_tibble() |>
            rename_with(~paste0(column_name, "_", .x)) |>
            list()
        })
      }) |> 
      bind_rows()
    
    if(unnest_columns) {
      output <- output |> 
        select(
          -metadata, -round
        ) |> 
        unnest(
          c(compSeason, home, away, venue)
          )
      
    }
  }
  
  output
}

match_metadata_afl <- round_metadata_afl |> 
  select(competition_id, season_id, roundNumber, round_id = id, year, is_final) |> 
  mutate(
    output = pmap(list(roundNumber, competition_id, season_id), 
                  get_match_info, convert_to_df = TRUE, unnest_columns = FALSE)
    ) |> 
  unnest(output) |>
  select(
    -metadata, -round
    ) |> 
  unnest(
    c(compSeason, home, away, venue)
  ) |> 
  mutate(
    match_start_time = as.POSIXct(utcStartTime, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")
    )

write_parquet(match_metadata_afl, "metadata/data/processed/match_metadata_afl.parquet")
