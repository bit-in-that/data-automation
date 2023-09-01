library(bit.data)
library(dplyr)
library(purrr)
library(fst)

content_creators <- data.table::fread("fantasy_rankings/data/raw/content_creators.csv") |> as_tibble()
content_organisations <- data.table::fread("fantasy_rankings/data/raw/content_organisations.csv") |> as_tibble()
overall_ranks_2022_top2550 <- read_fst("fantasy_rankings/data/processed/overall_ranks_2022_top2550.fst") |> select(user_id, rank_2022 = rank)
overall_ranks_2023 <- read_fst("fantasy_rankings/data/processed/overall_ranks_2023.fst") |> select(user_id, rank_2023 = rank)

  
content_creators |> 
  filter(include) |>
  left_join(content_organisations, "organisation") |> 
  left_join(overall_ranks_2022_top2550, "user_id") |> 
  left_join(overall_ranks_2023, "user_id") -> content_creators_ranks

# "https://fantasy.afl.com.au/assets/media/avatars/afl/2673766.png?v=0"

write_fst(content_creators_ranks, "fantasy_rankings/data/processed//content_creators_ranks.fst")


