library(arrow)
library(dplyr)

source("aflw_fantasy/modules/get_squad_data_afw.R")
source("aflw_fantasy/modules/get_rank_data_afw.R")
squad_data <- get_squad_data()
# write_parquet(squad_data, "aflw_fantasy/data/processed/squad_data.parquet")

session_id <- "533aac36e90c0ae896cb3a89_1696920884"

source("aflw_fantasy/modules/get_afw_session_id.R")
session_id <- get_afw_session_id()


rankings_data <- get_ranking_data(session_id, squad_data)

write_parquet(ranking_data, "aflw_fantasy/data/processed/ranking_data.parquet")

ranking_data <- read_parquet("aflw_fantasy/data/processed/ranking_data.parquet")

save_ranking_data(session_id)
