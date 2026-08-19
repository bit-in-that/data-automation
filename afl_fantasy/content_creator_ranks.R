# library(bit.data)
library(dplyr)
library(purrr)
# library(fst)
library(arrow)

content_creators <- data.table::fread("afl_fantasy/data/raw/content_creators.csv") |> as_tibble()
content_organisations <- data.table::fread("afl_fantasy/data/raw/content_organisations.csv") |> 
  as_tibble()

overall_ranks_2022_top2550 <- read_parquet("afl_fantasy/data/processed/overall_ranks_2022_top2550.parquet") |> 
  select(user_id, rank_2022 = rank)

overall_ranks_2023 <- read_parquet("afl_fantasy/data/processed/overall_ranks_2023.parquet") |> 
  select(user_id, rank_2023 = rank)

overall_ranks_2024 <- read_parquet("afl_fantasy/data/raw/2024/afl_fantasy_rankings.parquet") |> 
  select(user_id, rank_2024 = rank)

afl_fantasy_team_ids <- read_parquet("afl_fantasy/data/raw/2024/afl_fantasy_team_ids.parquet") |> 
  select(user_id, team_name, team_id)

overall_ranks_combined_2022_2023 <- read_parquet("afl_fantasy/data/processed/overall_ranks_combined_2022_2023.parquet")

content_creators_ranks <- content_creators |> 
  select(-team_name) |> 
  filter(include) |>
  left_join(afl_fantasy_team_ids, "user_id") |> 
  left_join(content_organisations, "organisation") |> 
  left_join(overall_ranks_2022_top2550, "user_id") |> 
  left_join(overall_ranks_2023, "user_id") |> 
  left_join(overall_ranks_2024, "user_id") |> 
  relocate(team_name, .after = name)

write_parquet(content_creators_ranks, "afl_fantasy/data//processed/content_creators_ranks.parquet")
# write a script for getting the current rand eventually
