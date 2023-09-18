library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)

get_afw_rounds <- function(expand_matches = FALSE) {
  response <- GET("https://aflwfantasy.afl/json/fantasy/rounds.json")
  output <- response |> 
    content(as = "text", encoding = "UTF-8") |> 
    fromJSON(flatten = TRUE) |> 
    as_tibble() |> 
    rename(round_id = id, round_status = status)
  
  if(expand_matches) {
    output |> 
      unnest(tournaments)
  } else {
    output |> 
      select(-tournaments)
  }
}

get_current_round <- function() {
  get_afw_rounds() |> 
    filter(round_status == "complete") |> 
    pull(round_id) |> 
    max()
}

