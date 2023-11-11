library(dplyr)
library(httr)
library(tidyr)
library(purrr)
library(arrow)
library(xml2)
library(jsonlite)

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


get_sanfl_ladder_history <- function(comp, year_to = 2023) {
  tibble(
    comp = c("sanfl", "womens", "reserves", "u18", "u16"),
    year_from = if_else(comp %in% c("u16", "womens"), 2017L, 2015L),
    years = map(year_from, seq, to = year_to)
    ) |> 
    mutate(
      ladder = map2(comp, years, ~{
        .y |> 
          map(\(year){
            get_sanfl_ladder(year, comp = .x) |> 
              mutate(year = year)
            }) |> 
          list_rbind()
        })
    ) |> 
    unnest(ladder) |> 
    relocate(year, .after = "comp") |> 
    select(-c("year_from", "years"))

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
  if(is(player_data_list, "xml_document")) { #access denied error
    return(NULL)
  }
  
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
    filter(!is.na(playerId), season_year >= 2021L) |> 
    distinct(playerId) |> 
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

get_sanfl_theme_vars <- function() {
  # this is a URL for random player, there is a json embeded within the site source
  response <- GET("https://sanfl.com.au/league/clubs/glenelg/1015506")
  
  output <- content(response)
  
  xml_find_all(output, ".//script")[[2]] |> 
    xml_text() |> 
    str_remove("^(.|\n)*themeVars = ") |> 
    str_remove("\\s+$") |> 
    parse_json()
}

get_sanfl_clubs_info <- function() {
  json_list <- get_sanfl_theme_vars()
  json_list$clubsInfo |> 
    bind_rows() |> 
    rename(club_id = id, club_name = name, club_link = link) |>
    select(-clubSite)
}

transform_sanfl_league_info <- function(leaguesInfo) {
  leaguesInfo[["sponsor"]] <- NULL
  
  leaguesInfo |>
    as_tibble() |>
    mutate(
      seasons = map(seasons, \(season_data) {
        season_data[["clubs"]] <- NULL
        season_data[["supports"]] <- NULL
        
        
        season_data |>
          as_tibble() |>
          mutate(
            finalsRounds = map(finalsRounds, ~{
              if(isFALSE(.x)) {
                tibble(name = NA, shortName = NA)
              } else {
                as_tibble(.x)
              }
              
              })
          ) |>
          unnest(finalsRounds) |>
          rename_with(~paste0("finals_", .x), ends_with("Name")) |>
          rename_with(~paste0("season_", .x), !ends_with("Name"))
      })
    ) |>
    rename_with(~ paste0("comp_", .x), !"seasons") |>
    unnest(seasons)
}

get_sanfl_season_finals_info <- function() {
  json_list <- get_sanfl_theme_vars()
  
  json_list$leaguesInfo |>
    map(transform_sanfl_league_info) |>
    bind_rows() |>
    group_by(comp_id, season_key) |>
    mutate(
      roundNumber = as.character(seq(n()) + 100)
    ) |>
    ungroup()
}

save_sanfl_metadata <- function() {
  sanfl_clubs_info <- get_sanfl_clubs_info()
  sanfl_season_finals_info <- get_sanfl_season_finals_info()
  
  write_parquet(sanfl_clubs_info, "state_leagues/data/raw/sanfl_clubs_info.parquet")
  write_parquet(sanfl_season_finals_info, "state_leagues/data/raw/sanfl_season_finals_info.parquet")
  
}
