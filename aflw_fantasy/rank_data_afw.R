library(arrow)

source("aflw_fantasy/modules/get_squad_data_afw.R")
source("aflw_fantasy/modules/get_rank_data_afw.R")
squad_data <- get_squad_data()
# write_parquet(squad_data, "aflw_fantasy/data/processed/squad_data.parquet")

session_id <- "82f7e91375964cc93197f067_1693810312"

rankings_data <- get_rank_data(session_id, squad_data)

write_parquet(ranking_data, "aflw_fantasy/data/processed/ranking_data.parquet")

ranking_data <- read_parquet("aflw_fantasy/data/processed/rankings_data.parquet")

save_ranking_data(session_id)
