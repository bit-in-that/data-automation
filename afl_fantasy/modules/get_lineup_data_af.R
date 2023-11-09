library(httr)
library(purrr)
library(dplyr)
library(tidyr)
library(arrow)
library(bit.data)

# player_data <- readRDS("afl_fantasy/data/raw/2023/player_data.RDS")

get_af_my_team_raw <- function(session_id, round_num = NULL) {
  headers = c(
    cookie = paste0("X-SID=", session_id, ";")
  )
  
  query <- list(round = round_num)
  
  response <- GET(url = "https://fantasy.afl.com.au/afl_classic/api/teams_classic/show_my", set_cookies(session = session_id), query = query)
  
  output <- content(response)
  
  stopifnot(identical(output$errors, list()))
  
  c(output$result, list(snapshotTime = Sys.time()))
    
}

get_af_team_raw <- function(session_id, team_id,  round_num = NULL) {
  headers = c(
    cookie = paste0("X-SID=", session_id, ";")
  )
  query <- list(id = team_id, round = round_num)
  
  response <- GET(url = "https://fantasy.afl.com.au/afl_classic/api/teams_classic/show", query = query,
                  set_cookies(session = session_id))
  
  output <- content(response)
  
  stopifnot(identical(output$errors, list()))
  
  c(output$result, list(snapshotTime = Sys.time()))
  
}


convert_lineup_list_to_df <- function(lineup_list, round_num = NULL) {
    team_name <- lineup_list$name
    user_id <- lineup_list$user_id
    team_id <- lineup_list$id
    if(is.null(round_num)) {
      round <- lineup_list$scoreflow |> names() |> as.integer() |> max()
    } else {
      round <- round_num
    }
    if(round < lineup_list$start_round) {
      return(NULL)
    }
    
    round <- as.character(round)
    
    score = lineup_list$scoreflow[[round]]
    
    captain <- lineup_list$lineup$captain
    vice_captain <- lineup_list$lineup$vice_captain
    
    if(is.null(lineup_list$firstname)) {
      first_name <- NA_character_
      last_name <- NA_character_
    } else {
      first_name <- lineup_list$firstname 
      last_name <- lineup_list$lastname 
    }

  on_field_players <- lineup_list$lineup[1:4] |>
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
  
  bench_players <- lineup_list$lineup$bench[1:4] |> 
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
  
  emergencies <- as.integer(lineup_list$lineup$bench$emergency)
  
  on_field_players |> 
    bind_rows(
      bench_players
    ) |> 
    mutate(
      team_name = team_name,
      first_name = first_name,
      last_name = last_name,
      user_id = user_id,
      team_id = team_id,
      round = round,
      score = score,
      value = lineup_list$value,
      salary_cap = lineup_list$salary_cap,
      salary_remaining = salary_cap - value,
      formation = lineup_list$formation,
      activated_at = lineup_list$activated_at,
      valid = lineup_list$valid,
      complete_first_time = lineup_list$complete_first_time,
      snapshot_time = lineup_list$snapshotTime
    ) |> 
    relocate(c("player_id", "positon", "is_bench"), .after = "snapshot_time") |> 
    mutate(
      is_captain = player_id %in% captain,
      is_vice_captain = player_id %in% vice_captain,
      is_emergency = player_id %in% emergencies
    )
}


transform_multiple_lineups <- function(lineup_list, player_data_by_round = get_player_data(by_round = TRUE)) {
  lineup_list |> 
    mutate(
      is_defender = positon == "DEF",
      is_midfielder = positon == "MID",
      is_ruck = positon == "RUC",
      is_forward = positon == "FWD"
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
      ranking_data |> select(user_id = userId, overall_rank = overallRank),
      by = "user_id"
    )
}

save_top_10000_lineups <- function(session_id) {
  round_number <- get_current_round()
  
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

# TESTING:
session_id <- "81a0ffb18849cce788c5d368d31047c772e2e99d"


x <- get_af_my_team_raw(session_id)

# my team id: 3061
# my user id: 1258257

# benji's bashers team id: 34706

convert_lineup_list_to_df(x)

y <- get_af_team_raw(session_id, 160518)

y <- get_af_team_raw(session_id, 160518, 20)

convert_lineup_list_to_df(y, 20) |> View()

convert_lineup_list_to_df(y, 20)

# 2813788
# 160518

af_lineups_data <- tibble(
  team_id = c(34706, 3061, 160518)
) |> 
  mutate(
    af_team = map(team_id, \(team_id) {
      map(1:24, \(round_num) {
        get_af_team_raw(session_id, team_id, round_num) |> 
          convert_lineup_list_to_df(round_num)
      }
        
      ) |> 
        list_rbind()
      })
  ) |> 
  pull(af_team) |> 
  list_rbind()



overall_ranks_2023 <- read_parquet("afl_fantasy/data/processed/overall_ranks_2023.parquet")
content_creators_ranks <- read_parquet("afl_fantasy/data/processed/content_creators_ranks.parquet")




system.time({
  af_lineups_data_top5000 <- tibble(
    team_id = union(overall_ranks_2023$team_id[1:5000],content_creators_ranks$team_id_2023)
  ) |> 
    mutate(
      af_team = map(team_id, \(team_id) {
        map(1:24, \(round_num) {
          get_af_team_raw(session_id, team_id, round_num) |> 
            convert_lineup_list_to_df(round_num)
        }
        
        ) |> 
          list_rbind()
      })
    ) |> 
    pull(af_team) |> 
    list_rbind()
})
# took about 2 hours
# user  system elapsed 
# 1441.26   31.05 6909.10

write_parquet(af_lineups_data_top5000, "afl_fantasy/data/raw/2023/af_lineups_data_top5000.parquet")
write_parquet(af_lineups_data, "afl_fantasy/data/raw/2023/af_lineups_data.parquet")


