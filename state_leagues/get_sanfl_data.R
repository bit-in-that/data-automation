library(dplyr)
library(httr)
library(tidyr)
library(purrr)

# years 2015 onwards, teamCodes in get_sanfl_ladder()$teamCode   
# comps in sanfl, womens, reserves, u18, u16

get_sanfl_ladder <- function(year = 2023, comp = "sanfl") {
  response <- GET(paste0("https://api3.sanflstats.com/ladder/", year, "/", comp))
  
  output <- content(response)
  
  stopifnot(is.list(output))
  
  output |> 
    bind_rows(list()) |> 
    mutate(
      ladder = map(ladder, as_tibble)
    ) |> 
    unnest(ladder)
}

get_sanfl_player_details <- function(teamCode, year = 2023, comp = "sanfl") {
  
  response <- GET(paste0("https://api3.sanflstats.com/players/", teamCode, "/", year, "/", comp))
  
  
  output <- content(response)
  
  stopifnot(is.list(output))
  
  output$players |> 
    map(~ {
      if(isTRUE(.x$weight == 0L)) {
        .x$weight <- NA_character_
      }
      if(isTRUE(.x$height == 0L)) {
        .x$height <- NA_character_
      }
      if("age" %in% names(.x)) {
        .x$age <- as.integer(.x$age)
      }
      .x
    }) |> 
    bind_rows()
}


get_sanfl_player_details_comp_history <- function(comp, year_to = 2023) {
  if(comp %in% c("u16", "womens")) {
    year_from <- 2017
    
  } else {
    year_from <- 2015
    
  }
  years <- seq(year_from, year_to)
  
  years |> 
    map(get_sanfl_ladder, comp = comp) |> 
    map2(years, ~ {
      map(.x$teamCode, get_sanfl_player_details, year = .y, comp = comp) |> 
        list_rbind() |> 
        mutate(season_year = .y)
      }) |>
    list_rbind() |>
    mutate(competition = comp)
  
}

get_sanfl_player_details_all_history <- function(year_to = 2023) {
  c("sanfl", "womens", "reserves", "u18", "u16") |> 
    map(get_sanfl_player_details_comp_history, year_to = year_to) |> 
    list_rbind()
  
}

get_sanfl_player_stats <- function(player_id, if_modified_since = NULL) { #format for if_modified_since: "Fri, 18 Aug 2023 00:25:40 GMT"
  headers = c(
    'if-modified-since' = if_modified_since
  )
  response <- GET(paste0("https://api3.sanflstats.com/player/", player_id), add_headers(headers))
  
  output <- content(response)
  
  
  output
}

x <- get_sanfl_player_stats("1017051")
x <- get_sanfl_player_stats("1009516")

x$players[[1]]$careerStats[[4]]$matchesPlayed

x$players[[1]] |>
  str()

# output round by round stats

# =============================================================================

system.time({
  player_details_sanfl <- get_sanfl_player_details_all_history()
})

player_details_sanfl |> 
  filter(season_year == 2023, competition == "u16") |> View()
  pull(playerId) |> 
  unique() |> 
  length()


write_parquet(player_details_sanfl, "state_leagues/data/raw/player_details_sanfl.parquet")

