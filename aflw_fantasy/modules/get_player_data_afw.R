library(httr)
library(purrr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(arrow)

get_player_data <- function(by_round = FALSE) {
  
  response <- GET(url = "https://aflwfantasy.afl/json/fantasy/players.json")
  
  output <- content(response)
  
  player_data_flat <- output |> 
    map(~map_if(.x, is.list,  ~list(.x))) |> 
    bind_rows() |> 
    mutate(
      squad = map(squad, bind_cols),
      position = unlist(position),
      stats = map(stats, ~map_if(.x, is.list,  ~list(.x))) |> 
        map(~ bind_rows(.x, list()))
    ) |> 
    unnest(squad, names_sep = "_") |> 
    unnest(stats) |>
    mutate(
      full_name = paste(firstName, lastName)
    ) |> 
    mutate(
      selections_snapshot_time = Sys.time()
    ) |> 
    relocate(
      selections_snapshot_time, .after = "selections"
    )
  
  if("scores" %in% names(player_data_flat)) {
    # At the start of the season, the scores column might not exist, need to be careful of this.
    player_data_flat <- player_data_flat |>  
      mutate(
        scores = map_if(scores, ~!is.null(.x), ~tibble(round = as.integer(names(.x)), score = as.integer(.x)))
        )
    
  } else {
    player_data_flat <- player_data_flat |> 
      mutate(
        scores = list(NULL)
      )
  }
  
  if("prices" %in% names(player_data_flat)) {
    # At the start of the season, the scores column might not exist, need to be careful of this.
    player_data_flat <- player_data_flat |>  
      mutate(
        prices = map_if(prices, ~!is.null(.x), ~{
          out <- tibble(round = as.integer(names(.x)), round_price = as.integer(.x))
          if(1L %in% out$round) {
            out
          } else {
            all_rounds <- out |> pull(round)
            missing_rounds <- setdiff(1:max(all_rounds), all_rounds)
            first_round_price <- out |> pull(round_price) |> tail(n = 1)
            bind_rows(
              out, tibble(round = missing_rounds, round_price = first_round_price)
            )
          }
        })
        )
    
  } else {
    player_data_flat <- player_data_flat |> 
      mutate(
        prices = list(NULL)
      )
  }
  
  if(by_round) {
    player_data_flat |> 
      mutate(
        round_stats = map2(prices, scores, ~{
          if(is.null(.y)) {
            .x |> 
              mutate(
                score = NA_integer_
              )
          } else {
            left_join(.x, .y, "round")
          }
        })
      ) |> 
      select(-prices, -scores) |>
      unnest(round_stats)
  } else {
    player_data_flat |> 
      select(-prices, -scores)
    
  }
  
}

save_player_selections <- function(suffix = format(Sys.time(), "%Y-%m-%d_%H-%M-%S"), player_data = get_player_data()) {
  player_data |> 
    select(id, selections) |> 
    write_parquet(paste0("aflw_fantasy/data/raw/afw_player_selections_", suffix, ".parquet")) 
  
}

save_player_selections_teams <- function() {
  source("aflw_fantasy/modules/get_rounds_afw.R") # uses jsonlite
  
  round_number <- get_current_round()
  
  save_player_selections(paste0("teams_round", round_number + 1))
  
}

save_player_selections_lockout <- function() {
  source("aflw_fantasy/modules/get_rounds_afw.R") # uses jsonlite
  
  round_number <- get_current_round()
  
  save_player_selections(paste0("lockout_round", round_number + 1))
  
}
