library(bit.data)
library(fst)

team_count <- 163527L # 163527L
session_id <- "e3526fbe4f1f94016a39a5a58cba41c5f744ea59"
team_ids <- 1L:team_count
userids <- integer(team_count)
ranks <- integer(team_count)
lineup_single <- list()
lineups_list <- list()
ranking_single <- list()
rankings_list <- list()

for(team_id in team_ids) {
  lineup_single <- get_af_classic_lineup_raw(session_id = session_id, team_id)
  lineups_list[[team_id]] <- lineup_single
  ranking_single <- get_af_user_rank_raw(session_id = session_id, lineup_single$user_id)
  rankings_list[[team_id]] <- ranking_single
  userids[team_id] <- lineup_single$user_id
  ranks[team_id] <- ranking_single$rank
}

ranking_data <- data.frame(
  team_id = team_ids,
  userid = userids,
  rank = ranks
)

dir.create("outputs/fantasy_rankings", recursive = TRUE)

saveRDS(lineups_list, "outputs/fantasy_rankings/lineups_list.RDS")
saveRDS(rankings_list, "outputs/fantasy_rankings/rankings_list.RDS")
write_fst(ranking_data, "outputs/fantasy_rankings/ranking_data.fst")
