library(httr)
library(purrr)
library(dplyr)
library(tidyr)
library(arrow)

source("aflw_fantasy/modules/get_player_data_afw.R")

# session_id <- "ff36262fdd7c030c61ec4e85_1692610963"

get_afw_my_team_raw <- function(session_id) {
  headers = c(
    cookie = paste0("X-SID=", session_id, ";")
  )
  
  response <- GET(url = "https://aflwfantasy.afl/api/en/fantasy/team/show-my", add_headers(headers))
  
  output <- content(response)
  
  stopifnot(identical(output$errors, list()))
  
  c(output$success$team, list(rollbackAvailable = output$success$rollbackAvailable, snapshotTime = Sys.time()))
}

get_afw_team_raw <- function(team_id, round, session_id) {
  headers = c(
    cookie = paste0("X-SID=", session_id, ";")
  )
  
  response <- GET(url = paste0("https://aflwfantasy.afl/api/en/fantasy/team-history/get-by-round-and-user/", round, "/", team_id), 
                  add_headers(headers))
  
  output <- content(response)
  
  stopifnot(identical(output$errors, list()))
  
  c(output$success$teamHistory, list(snapshotTime = Sys.time()))
  
}


convert_lineup_list_to_df <- function(lineup_list, is_my_lineup = FALSE, my_round = NULL) {
  if(is_my_lineup) {
    team_name <- my_lineup_list$name
    user_id <- lineup_list$userId
    team_id <- lineup_list$id
    if(is.null(my_round)) {
      round <- lineup_list$startRoundId
    } else {
      round <- my_round
    }
    captain <- lineup_list$captainId
    vice_captain <- lineup_list$viceCaptainId
    utility <- lineup_list$utilityId
  } else {
    team_name <- lineup_list$teamName
    user_id <- lineup_list$userId
    team_id <- lineup_list$teamId
    round <- lineup_list$roundId
    captain <- lineup_list$captain
    vice_captain <- lineup_list$viceCaptain
    utility <- lineup_list$utility
  }
  
  on_field_players <- lineup_list$lineup |>
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
  
  bench_players <- lineup_list$bench |> 
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
  
  on_field_players |> 
    bind_rows(
      bench_players, 
      tibble(
        player_id = utility,
        positon = "UTL",
        is_bench = TRUE
      )
    ) |> 
    mutate(
      team_name = team_name,
      user_id = user_id,
      team_id = team_id,
      round = round,
      value = lineup_list$value,
      salary_cap = lineup_list$salaryCap,
      salary_remaining = salary_cap - value,
      snapshot_time = lineup_list$snapshotTime
    ) |> 
    relocate(c("player_id", "positon", "is_bench"), .after = "snapshot_time") |> 
    mutate(
      is_captain = player_id %in% captain,
      is_vice_captain = player_id %in% vice_captain
    )
}


transform_multiple_lineups <- function(lineup_list, player_data_by_round = get_player_data(by_round = TRUE)) {
  lineup_list |> 
    mutate(
      is_defender = positon == "DEF",
      is_midfielder = positon == "MID",
      is_ruck = positon == "RUC",
      is_forward = positon == "FWD",
      is_utility = positon == "UTL"      
    ) |> 
    left_join(
      player_data_by_round |> 
        select(round, player_id = id, player_name = full_name, player_score = score, 
               player_average = avgPoints, player_cost = cost, player_price_round = round_price,
               player_selections = selections, 
        ), 
      by = c("player_id", "round")
    )
}


get_top_n_lineups <- function(n_teams, round_number, session_id, ranking_data = read_parquet("aflw_fantasy/data/processed/ranking_data.parquet"), player_data_by_round = get_player_data(by_round = TRUE)) {
  ranking_data |> 
    filter(overallRank %in% seq(n_teams)) |> 
    pull(userId) |> 
    map(get_afw_team_raw, round = round_number, session_id = session_id) |>
    map(convert_lineup_list_to_df) |> 
    list_rbind() |> 
    transform_multiple_lineups(player_data_by_round) |> 
    left_join(
      ranking_data |> select(user_id = userId, team_name = teamName, overall_rank = overallRank),
      by = "user_id"
    )
}

save_top_10000_lineups <- function(session_id, round_number) {
  top_10000_lineups <- get_top_n_lineups(10000, round_number, session_id) 
  
  write_parquet(top_10000_lineups, "aflw_fantasy/data/raw/top_10000_lineups.parquet")
  
  top_10000_selections <- top_10000_lineups |> 
    group_by(
      player_id, player_name, player_score, player_selections, player_cost
    ) |> 
    summarise(
      selections_top_10000 = sum(overall_rank <= 10000) / 10000,
      selections_top_5000 = sum(overall_rank <= 5000) / 5000,
      selections_top_2000 = sum(overall_rank <= 2000) / 2000,
      selections_top_1000 = sum(overall_rank <= 1000) / 1000,
      selections_top_100 = sum(overall_rank <= 100) / 100,
      captain_top_10000 = sum(overall_rank <= 10000 & is_captain) / 10000,
      captain_top_5000 = sum(overall_rank <= 5000 & is_captain) / 5000,
      captain_top_2000 = sum(overall_rank <= 2000 & is_captain) / 2000,
      captain_top_1000 = sum(overall_rank <= 1000 & is_captain) / 1000,
      captain_top_100 = sum(overall_rank <= 100 & is_captain) / 100,
      vice_captain_top_10000 = sum(overall_rank <= 10000 & is_vice_captain) / 10000,
      vice_captain_top_5000 = sum(overall_rank <= 5000 & is_vice_captain) / 5000,
      vice_captain_top_2000 = sum(overall_rank <= 2000 & is_vice_captain) / 2000,
      vice_captain_top_1000 = sum(overall_rank <= 1000 & is_vice_captain) / 1000,
      vice_captain_top_100 = sum(overall_rank <= 100 & is_vice_captain) / 100,
      bench_top_10000 = sum(overall_rank <= 10000 & is_bench) / 10000,
      bench_top_5000 = sum(overall_rank <= 5000 & is_bench) / 5000,
      bench_top_2000 = sum(overall_rank <= 2000 & is_bench) / 2000,
      bench_top_1000 = sum(overall_rank <= 1000 & is_bench) / 1000,
      bench_top_100 = sum(overall_rank <= 100 & is_bench) / 100,
      utility_top_10000 = sum(overall_rank <= 10000 & is_utility) / 10000,
      utility_top_5000 = sum(overall_rank <= 5000 & is_utility) / 5000,
      utility_top_2000 = sum(overall_rank <= 2000 & is_utility) / 2000,
      utility_top_1000 = sum(overall_rank <= 1000 & is_utility) / 1000,
      utility_top_100 = sum(overall_rank <= 100 & is_utility) / 100,
      .groups = "drop"
    )
  
  write_parquet(top_10000_selections, "aflw_fantasy/data/processed/top_10000_selections.parquet")
}
