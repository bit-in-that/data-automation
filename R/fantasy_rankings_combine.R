library(fst)
library(dplyr)
library(purrr)

ranking_data_paths <- dir(path = "outputs/fantasy_rankings/", full.names = TRUE)


ranking_data_paths |> 
  map_dfr(read_fst) |> 
  add_row( # for some reason this one was skipped
    team_id = 5430, userid = 2391264,  rank = 70909, score = 42608
    ) |> 
  filter(!is.na(rank)) |> 
  arrange(rank) -> combined_data

top_rankings_overall <- readRDS("../bit.data/dev/2022_data/top_rankings_overall.RDS")


top_rankings_overall |> 
  distinct(user_id, rank, league_points, team_name) |>
  left_join(combined_data, c("user_id" = "userid"), suffix = c("", "_2023")) |> 
  data.table::fwrite(file = "test.csv")



combined_data |> 
  filter(is.na(rank)) |> 
  View()
