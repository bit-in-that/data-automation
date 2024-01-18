library(jsonlite)

source("_examples/modules/afl_fantasy_apis.R")

source("afl_fantasy/modules/get_af_session_id.R")
session_id <- get_af_session_id()

celebrities <- get_afl_fantasy_celebrities(session_id)
write_json(celebrities, "_examples/output/celebrities.json", pretty = TRUE)

my_leagues <- get_afl_fantasy_my_leagues(session_id)
write_json(my_leagues, "_examples/output/my_leagues.json", pretty = TRUE)

my_lineup <- get_afl_fantasy_my_lineup(session_id)
write_json(my_lineup, "_examples/output/my_lineup.json", pretty = TRUE)

lineup <- get_afl_fantasy_lineup(session_id, 1)
write_json(lineup, "_examples/output/lineup.json", pretty = TRUE)

favourites <- get_afl_fantasy_favourites(session_id)
write_json(favourites, "_examples/output/favourites.json", pretty = TRUE)

trades_history <- get_afl_fantasy_trades_history(session_id)
write_json(trades_history, "_examples/output/trades_history.json", pretty = TRUE)

user <- get_afl_fantasy_user(session_id)
write_json(user, "_examples/output/user.json", pretty = TRUE)

rounds <- get_afl_fantasy_rounds()
write_json(rounds, "_examples/output/rounds.json", pretty = TRUE)

players <- get_afl_fantasy_players()
write_json(players, "_examples/output/players.json", pretty = TRUE)

players2 <- get_afl_fantasy_players2()
write_json(players2, "_examples/output/players2.json", pretty = TRUE)

single_player <- get_afl_fantasy_single_player(1004592)
write_json(single_player, "_examples/output/single_player.json", pretty = TRUE)

venues <- get_afl_fantasy_venues()
write_json(players, "_examples/output/venues.json", pretty = TRUE)

squads <- get_afl_fantasy_squads()
write_json(squads, "_examples/output/squads.json", pretty = TRUE)

team_rank_history <- get_afl_fantasy_team_rank_history(session_id, user_id = 2673766) #1258257
write_json(team_rank_history, "_examples/output/team_rank_history.json", pretty = TRUE)

rankings <- get_afl_fantasy_rankings(session_id)
write_json(rankings, "_examples/output/rankings.json", pretty = TRUE)

player_preview <- get_afl_fantasy_player_preview()
write_json(player_preview, "_examples/output/player_preview.json", pretty = TRUE)

players_venues_stats <- get_afl_fantasy_players_venues_stats()
write_json(players_venues_stats, "_examples/output/players_venues_stats.json", pretty = TRUE)

players_opponents_stats <- get_afl_fantasy_players_opponents_stats()
write_json(players_opponents_stats, "_examples/output/players_opponents_stats.json", pretty = TRUE)

round_stats <- get_afl_fantasy_round_stats(round_num = 1)
write_json(round_stats, "_examples/output/round_stats.json", pretty = TRUE)
