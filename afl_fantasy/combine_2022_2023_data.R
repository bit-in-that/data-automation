library(fst)
library(arrow)
library(dplyr)
library(purrr)


top2550_rank_history_2022 <- readRDS("afl_fantasy/data/raw/2022/top2550_rank_history_2022.RDS")
ranking_data_paths <- dir(path = "afl_fantasy/data/raw/2023/batched_overall_ranks/", full.names = TRUE)
afl_fantasy_team_ids <- read_parquet("afl_fantasy/data/raw/2024/afl_fantasy_team_ids.parquet") |> 
  select(user_id, team_id, team_name_new = team_name) 


# combine the 2023 data
ranking_data_paths |> 
  map_dfr(read_fst) |> 
  add_row( # for some reason this one was skipped
    team_id = 5430, userid = 2391264,  rank = 70909, score = 42608
    ) |> 
  filter(!is.na(rank)) |> 
  rename( # make a bit of a mistake with the names here
    user_id = userid
  ) |> 
  arrange(rank) -> overall_ranks_2023


# manipulate the 2022 data
top2550_rank_history_2022 |> 
  filter(overall_rank <= 2550, round == 23) |> 
  select(
    team_name, firstname, lastname, user_id, rank = overall_rank, score = league_points
  ) -> overall_ranks_2022_top2550

overall_ranks_combined_2022_2023 <- inner_join(overall_ranks_2023, overall_ranks_2022_top2550, "user_id", suffix = c("_2023", "_2022")) |> 
  # add 2024 id instead of 2023
  select(-team_id) |> 
  left_join(afl_fantasy_team_ids, "user_id") |> 
  # relocate(team_name, .before = firstname)|>
  mutate(
    team_name = coalesce(team_name_new, team_name)
  ) |> 
  select(-team_name_new) |> 
  relocate(team_id, .before = user_id)



write_parquet(overall_ranks_2023, "afl_fantasy/data/processed/overall_ranks_2023.parquet") 
write_parquet(overall_ranks_2022_top2550, "afl_fantasy/data/processed/overall_ranks_2022_top2550.parquet") 
write_parquet(overall_ranks_combined_2022_2023, "afl_fantasy/data/processed/overall_ranks_combined_2022_2023.parquet") 
