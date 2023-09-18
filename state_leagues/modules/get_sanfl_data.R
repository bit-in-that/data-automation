library(dplyr)
library(httr)
library(tidyr)
library(purrr)
library(arrow)

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

transform_sanfl_player_data <- function(player_data_list, career = FALSE) {
  player_data_tb <- player_data_list$players[[1]] |> 
    map_if(is.null, ~ NA_character_) |> 
    map_if(is.list, list, .else = as.character) |> 
    as_tibble()
  
  # return(player_data_tb)
  
  if("leagueCareerStats" %in% names(player_data_tb)) {
    player_data_tb <- player_data_tb |> 
      mutate(
        leagueCareerStats = leagueCareerStats[[1]] |> 
          purrr::list_flatten() |> 
          as_tibble() |> 
          list()
      ) |> 
      unnest(leagueCareerStats)
    
  }
    
  if(career) {
    
    player_data_tb |> 
      select(-careerStats, -currentSeasonStats) |> 
      mutate(
        careerTotalStats = careerTotalStats[[1]] |> 
          as_tibble() |> 
          list()
      ) |> 
      unnest(careerTotalStats)
    
  } else {
    player_data_tb |> 
      select(-careerTotalStats, -currentSeasonStats) |>
      mutate(
        careerStats = careerStats[[1]] |> 
          map(~ {
            .x |>
              map_if(is.null, ~ NA_character_) |> 
              map_if(is.list, list, .else = as.character) |> 
              (\(.x) {
                .x$clearances <- as.character(.x$clearances)
                .x
              })() |> 
              as_tibble() |> 
              mutate(
                matchStats = {
                  if(length(matchStats) == 0) {
                    list(NULL)
                  } else {
                    matchStats[[1]] |> 
                      map(~{
                        if("roundNumber" %in% names(.x)) {
                          .x$roundNumber <- as.character(.x$roundNumber)
                        }
                        .x
                      }) |>
                      bind_rows() |>
                      list()
                  }
                }              ) |> 
              rename_with(~ if(length(.x) == 0) character(0) else paste0("season_", .x), any_of(c("matchesPlayed", "behinds", "disposals", "dreamteamPoints", "freesAgainst", "freesFor", "goals", "handballs", "handballsReceived", "hitouts", "inside50s", "kickIneffective", "kickins", "kicks", "marks", "marksContested", "rebound50s", "tackles", "clearances"))) |> 
              unnest(matchStats)
          }) |> 
          bind_rows(list()) |> 
          list()
      ) |> 
      rename_with(~ {if(length(.x) == 0) character(0) else paste0("current_", .x)}, any_of(c("teamCode", "squadId"))) |>
      unnest(careerStats)
  }
}

save_sanfl_player_details <- function() {
  player_details_sanfl <- get_sanfl_player_details_all_history()
  write_parquet(player_details_sanfl, "state_leagues/data/raw/player_details_sanfl.parquet")
}

save_sanfl_player_stats <- function() {
  player_details_sanfl <- read_parquet("state_leagues/data/raw/player_details_sanfl.parquet")
  sanfl_player_stats <- player_details_sanfl |>
    filter(season_year == 2023, !is.na(playerId)) |> 
    distinct(playerId) |> 
    # BUG TESTING CODE:
    # filter(playerId == "1014624") |> 
    # slice_sample(n = 2) |>
    # {\(.x) {
    #   print(.x$playerId)
    #   .x
    # }}() |> 
    rename(player_id = playerId) |> 
    mutate(
      player_stats = map(player_id, get_sanfl_player_stats) |> 
        map(transform_sanfl_player_data)
    ) |> 
    unnest(player_stats)
  
  numerical_vairables <- names(sanfl_player_stats)[map_lgl(sanfl_player_stats, ~ str_detect(.x, "^\\-?\\d+$") |> all(na.rm = TRUE) && all(!is.na(.x)))] |> 
    str_subset(regex("id$", ignore_case = TRUE), negate = TRUE)
  
  sanfl_player_stats <- sanfl_player_stats |> 
    mutate(
      across(all_of(numerical_vairables), as.integer)
    )
  
  write_parquet(sanfl_player_stats, "state_leagues/data/raw/sanfl_player_stats.parquet")
}
