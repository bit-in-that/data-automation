library(bit.data)
library(fst)

get_ranking_data <- function(team_ids, data_suffix) {
  
  session_id <- "e3526fbe4f1f94016a39a5a58cba41c5f744ea59"
  userids <- integer(length(team_ids))
  ranks <- integer(length(team_ids))
  lineup_single <- list()
  # lineups_list <- list()
  ranking_single <- list()
  # rankings_list <- list()
  
  for(team_id in team_ids) {
    lineup_single <- get_af_classic_lineup_raw(session_id = session_id, team_id)
    # lineups_list[[team_id]] <- lineup_single
    ranking_single <- get_af_user_rank_raw(session_id = session_id, lineup_single$user_id)
    # rankings_list[[team_id]] <- ranking_single
    userids[team_id] <- lineup_single$user_id
    ranks[team_id] <- ranking_single$rank
  }
  
  ranking_data <- data.frame(
    team_id = team_ids,
    userid = userids,
    rank = ranks
  )
  
  
  # saveRDS(lineups_list, "outputs/fantasy_rankings/lineups_list.RDS")
  # saveRDS(rankings_list, "outputs/fantasy_rankings/rankings_list.RDS")
  
  suppressWarnings( dir.create("outputs/fantasy_rankings", recursive = TRUE))
  write_fst(ranking_data, paste0("outputs/fantasy_rankings/ranking_data", data_suffix, ".fst"))
  
}

# 163527L
# get_ranking_data(team_ids = 1L:20L, "_0s")

