library(httr)

hit_afl_fantasy_api <- function(api_url, session_id, query = list()) {
  response <- GET(api_url, set_cookies(session = session_id), query = query)
  
  output <- content(response)
  
  stopifnot(identical(output$errors, list()))
  
  output
}

get_afl_fantasy_celebrities <- function(session_id) {
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_classic/api/teams_classic/celebrities", session_id)
}

get_afl_fantasy_my_leagues <- function(session_id, limit = 50L) {
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_classic/api/leagues_classic/show_my", session_id, list(limit = limit))
}

get_afl_fantasy_my_lineup <- function(session_id) {
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_classic/api/teams_classic/show_my", session_id)
}

get_afl_fantasy_lineup <- function(session_id, id, round = NULL) {
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_classic/api/teams_classic/show", session_id, list(id = id, round = round))
}

get_afl_fantasy_favourites <- function(session_id) {
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_draft/api/players/favourites", session_id)
}

get_afl_fantasy_trades_history <- function(session_id) {
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_classic/api/teams_classic/show_trades_history", session_id)
}

get_afl_fantasy_user <- function(session_id) {
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_draft/api/user", session_id)
}

get_afl_fantasy_team_rank_history <- function(session_id, user_id = NULL) {
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_classic/api/teams_classic/snapshot", session_id, list(user_id = user_id))
}

get_afl_fantasy_rankings <- function(session_id, offset = 0, order = c("rank", "avg_points", "round_points", "highest_round_score", "team_value"), direction = c("DESC", "ASC"), club = NULL, state = NULL) {
  order <- order[1]
  direction <- direction[1]
  hit_afl_fantasy_api("https://fantasy.afl.com.au/afl_classic/api/teams_classic/rankings", session_id, list(offset = offset, order = order, order_direction = direction, club = club, state = state))
  
}

get_afl_fantasy_rounds <- function() {
  response <- GET("https://fantasy.afl.com.au/data/afl/rounds.json")
  content(response)
}

get_afl_fantasy_players <- function() {
  response <- GET("https://fantasy.afl.com.au/data/afl/players.json")
  content(response)
}

get_afl_fantasy_players2 <- function() {
  response <- GET("https://fantasy.afl.com.au/data/afl/coach/players.json")
  content(response)
}

get_afl_fantasy_single_player <- function(player_id) {
  response <- GET(paste0("https://fantasy.afl.com.au/data/afl/stats/players/", player_id, ".json"))
  content(response)
}

get_afl_fantasy_venues <- function() {
  response <- GET("https://fantasy.afl.com.au/data/afl/venues.json")
  content(response)
}

get_afl_fantasy_squads <- function() {
  response <- GET("https://fantasy.afl.com.au/data/afl/squads.json")
  content(response)
}

get_afl_fantasy_player_preview <- function() {
  response <- GET("https://fantasy.afl.com.au/data/afl/news/2022/player-preview.json")
  content(response)
}

get_afl_fantasy_players_venues_stats <- function() {
  response <- GET("https://fantasy.afl.com.au/data/afl/stats/players_venues_stats.json")
  content(response)
}

get_afl_fantasy_players_opponents_stats <- function() {
  response <- GET("https://fantasy.afl.com.au/data/afl/stats/players_opponents_stats.json")
  content(response)
}

get_afl_fantasy_round_stats <- function(round_num) {
  response <- GET(paste0("https://fantasy.afl.com.au/data/afl/stats/", round_num, ".json"))
  content(response)
}
