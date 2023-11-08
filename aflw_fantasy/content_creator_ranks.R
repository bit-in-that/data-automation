library(dplyr)
library(data.table)
library(arrow)

content_creators <- data.table::fread("aflw_fantasy/data/raw/content_creators_aflw.csv") |> as_tibble()
content_organisations <- data.table::fread("afl_fantasy/data/raw/content_organisations.csv") |> as_tibble()
rankings_data <- read_parquet("aflw_fantasy/data/processed/ranking_data.parquet") |> 
  select(
    user_id = userId,
    team_name = teamName,
    overall_rank = overallRank,
    total_points = totalPoints,
    roundPoints,
    roundRank 
  )


content_creators_ranks_aflw <- content_creators |> 
  filter(include) |>
  left_join(content_organisations, "organisation") |> 
  left_join(rankings_data, c("user_id"))

write_parquet(content_creators_ranks_aflw, "aflw_fantasy/data/processed/content_creators_ranks_aflw.parquet")
