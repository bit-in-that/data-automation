library(bit.data)
library(fst)

# 2813788

get_ranking_data <- function(team_ids, data_suffix, save_data = TRUE) {
  
  session_id <- "e3526fbe4f1f94016a39a5a58cba41c5f744ea59"
  userids <- integer(length(team_ids))
  ranks <- integer(length(team_ids))
  scores <- integer(length(team_ids))
  lineup_single <- list()
  # lineups_list <- list()
  ranking_single <- list()
  # rankings_list <- list()
  
  for(id in seq_along(team_ids)) {
    lineup_single <- try(get_af_classic_lineup_raw(session_id = session_id, team_ids[id]), silent = TRUE)
    # lineups_list[[team_id]] <- lineup_single
    if(!is(lineup_single, "try-error")) {
      ranking_single <- try(get_af_user_rank_raw(session_id = session_id, lineup_single$user_id), silent = TRUE)
      # rankings_list[[team_id]] <- ranking_single
      userids[id] <- lineup_single$user_id
      scores[id] <- lineup_single$points
      
      if(!is(ranking_single, "try-error")) {
        ranks[id] <- ranking_single$rank
      } else {
        ranks[id] <- NA_integer_
      }
      
    } else {
      userids[id] <- NA_integer_
      ranks[id] <- NA_integer_
      scores[id] <- NA_integer_
    }
  }
  
  ranking_data <- data.frame(
    team_id = team_ids,
    userid = userids,
    rank = ranks,
    score = scores
  )
  
  # saveRDS(lineups_list, "outputs/fantasy_rankings/lineups_list.RDS")
  # saveRDS(rankings_list, "outputs/fantasy_rankings/rankings_list.RDS")
  
  if(save_data) {
    suppressWarnings( dir.create("fantasy_rankings/data/raw/2023/batched_overall_ranks/", recursive = TRUE))
    write_fst(ranking_data, paste0("fantasy_rankings/data/raw/2023/batched_overall_ranks/", data_suffix, ".fst"))
    
  } else {
    ranking_data
  }
}

# 163550L
# get_ranking_data(team_ids = c(5430, 94180, 146593, 156418), "_0s")
