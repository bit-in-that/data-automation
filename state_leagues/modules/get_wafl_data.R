library(dplyr)
library(httr)
library(purrr)
library(arrow)
library(jsonlite)
library(tidyr)
library(xml2)

# need to create a mapping between AFL and WAFL ids
wafl_clubs_map <- c(
  "Claremont" = "63236960-4bd7-11e9-b622-65dd81c7aa9d",
  "East Fremantle" = "63274850-4bd7-11e9-a29d-bd0ecec49dc9",
  "East Perth" = "632ad0d0-4bd7-11e9-9ce6-c77f791bafcb",
  "Peel Thunder" = "632ebb80-4bd7-11e9-9660-19fd5993277e",
  "Perth" = "63326060-4bd7-11e9-995e-1be5c911bb6e",
  "South Australia" = "634d58c0-4bd7-11e9-b412-914628fae598",
  "South Fremantle" = "63360f40-4bd7-11e9-9a9f-f74d389400a7",
  "South West Talent Academy" = "90091230-4ef9-11ea-8c0d-b5aa69b7a7ca",
  "Subiaco" = "633991b0-4bd7-11e9-899f-63befd19faac",
  "Swan Districts" = "633d2a50-4bd7-11e9-93ca-5f98dc3ddb8e",
  "West Coast" = "63a5abf0-4bd7-11e9-acd2-39cf2c564a87",
  "West Perth" = "6340f450-4bd7-11e9-8cad-c97ae529e90a",
  "Western Australia" = "63449090-4bd7-11e9-9176-bb61af30e710"
)


get_wafl_player_player_list <- function(search = NULL, club = NULL, gender = NULL, type = NULL, all = NULL, limit = 100000) {
  # gender: "male" or "female"
  
  headers <- c(
    'authorization' = 'Bearer 773575be-2f11-4102-93e7-d3715e3e9c83'
  )
  
  query <- list(
    search = search,
    club = club,
    gender = gender,
    type = type,
    all = all,
    limit = limit
  )
  
  response <- GET("https://api.sportix.app/v1/site/players", add_headers(headers), query = query)
  
  content(response)$data

}

clean_wafl_player_details <- function(output_list) {
  output_list |> 
    map(~{
      club_list <- .x$club |> set_names("club_id", "club_name", "club_slug")
      
      .x$club <- NULL
      .x$age <- as.character(.x$age)
      c(.x, club_list)
    }) |> 
    bind_rows()
}


save_wafl_player_details <- function() {
  player_details_wafl <- letters |> 
    map(get_wafl_player_player_list) |> 
    do.call(what = c) |> 
    clean_wafl_player_details() |> 
    distinct(id, .keep_all = TRUE)
  
  player_details_wafl_current <- get_wafl_player_player_list() |> 
    clean_wafl_player_details()
  
  write_parquet(player_details_wafl, "state_leagues/data/raw/player_details_wafl.parquet")
  write_parquet(player_details_wafl_current, "state_leagues/data/raw/player_details_wafl_current.parquet")
}

get_wafl_player_stats <- function(wafl_player_id, type = "season", aggregate_matches = FALSE) {
  # type can also be "Career"
  headers <- c(
    'authorization' = 'Bearer 773575be-2f11-4102-93e7-d3715e3e9c83'
  )
  
  query <- list(
    id = wafl_player_id,
    type = type,
    sum = if(aggregate_matches) "true" else "false"
  )
  
  response <- GET("https://api.sportix.app/v1/site/player/statistics", add_headers(headers), query = query)
  
  output <- response |> 
    content(as = "text", encoding = "UTF-8") |> 
    fromJSON(flatten = TRUE)
  
  if(identical(output, list())) {
    NULL
  } else {
    output
  }
  
}

save_wafl_player_match_stats <- function() {
  wafl_id_conversion_table <- read_parquet("state_leagues/data/processed/wafl_id_conversion_table.parquet")
  
  wafl_player_stats <- wafl_id_conversion_table |> 
    mutate(
      game_stats = map(wafl_id, get_wafl_player_stats)
    ) |>
    unnest(game_stats)
    
  write_parquet(wafl_player_stats, "state_leagues/data/raw/wafl_player_stats.parquet")
  
}

get_wafl_player_image <- function(player_slug) {
  response <- GET(paste0("https://wafl.com.au/player/", player_slug))
  stopifnot(response$status_code == 200L)
  output <- content(response)
  
  output |> 
    xml_find_first("//*[contains(@class, 'player-info__photo')]//img") |> 
    xml_attr("src")
}
