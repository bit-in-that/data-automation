library(bit.data)
library(dplyr)
library(purrr)
library(fst)

content_creators <- data.table::fread("fantasy_rankings/data/raw/content_creators.csv") |> as_tibble()
content_organisations <- data.table::fread("fantasy_rankings/data/raw/content_organisations.csv") |> as_tibble()


content_creators |> 
  filter(include) |>
  left_join(content_organisations, "organisation") |> 
  mutate(
    # TODO: join this on using the ranks dataset
    rank_lists = map(user_id, get_af_user_rank_raw, session_id  = "e3526fbe4f1f94016a39a5a58cba41c5f744ea59"),
    score = map_int(rank_lists, ~.x$points),
    rank = map_int(rank_lists, ~.x$rank)
  ) |> 
  select(-rank_lists) -> content_creators_ranks

write_fst(content_creators_ranks, "fantasy_rankings/data/processed//content_creators_ranks.fst")


