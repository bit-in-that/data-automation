library(arrow)


# TODO: eventually turn this into a proper module instead or integrate with bit.in.that
source("aflw_fantasy/modules/get_player_data_afw.R")

player_data <- get_player_data()
player_data_by_round <- get_player_data(by_round = TRUE)

write_parquet(player_data, "aflw_fantasy/data/processed/player_data.parquet")
write_parquet(player_data_by_round, "aflw_fantasy/data/processed/player_data_by_round.parquet")

save_player_selections(player_data = player_data)

r2 <- read_parquet("aflw_fantasy/data/raw/afw_player_selectionslockout_round2.parquet")
# now <- read_parquet("aflw_fantasy/data/raw/afw_player_selections2023-09-05_01-55-08.parquet")


