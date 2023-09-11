library(bit.data)
library(dplyr)
library(purrr)
library(fst)
library(arrow)

content_creators <- data.table::fread("afl_fantasy/data/raw/content_creators.csv") |> as_tibble()
content_organisations <- data.table::fread("afl_fantasy/data/raw/content_organisations.csv") |> as_tibble()
overall_ranks_2022_top2550 <- read_parquet("afl_fantasy/data/processed/overall_ranks_2022_top2550.parquet") |> select(user_id, rank_2022 = rank)
overall_ranks_2023 <- read_parquet("afl_fantasy/data/processed/overall_ranks_2023.parquet") |> select(user_id, rank_2023 = rank)


content_creators |> 
  filter(include) |>
  left_join(content_organisations, "organisation") |> 
  left_join(overall_ranks_2022_top2550, "user_id") |> 
  left_join(overall_ranks_2023, "user_id") -> content_creators_ranks



write_parquet(content_creators_ranks, "afl_fantasy/data//processed/content_creators_ranks.parquet")
