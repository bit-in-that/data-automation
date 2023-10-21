library(httr)
library(arrow)
library(tidyr)
library(dplyr)
library(purrr)
library(stringr)

competition_metadata_afl <- read_parquet("metadata/data/processed/competition_metadata_afl.parquet")
season_metadata_afl <- read_parquet("metadata/data/processed/season_metadata_afl.parquet")

get_rounds <- function(season_id, page_size = 100) {
  response <- GET(paste0("https://aflapi.afl.com.au/afl/v2/compseasons/", season_id, "/rounds?pageSize=", page_size))
  content(response)
}

round_metadata_afl <- season_metadata_afl |> 
  transmute(
    competition_id,
    season_id = id,
    season_name = name,
    year,
    rounds = map(season_id, ~{
      get_rounds(.x)$rounds |> 
        map(~map_if(.x, is.list, list)) |> 
        bind_rows()
      })
    ) |> 
  bind_rows() |> 
  unnest(rounds) |> 
  mutate(
    has_byes  = !map_lgl(byes, identical, y = list()),
    is_final = !str_detect(name, "^Round "),
    start_time = as.POSIXct(utcStartTime, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC"),
    end_time = as.POSIXct(utcEndTime, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")
  ) |> 
  select(-c("byes", "utcStartTime", "utcEndTime"))


write_parquet(round_metadata_afl, "metadata/data/processed/round_metadata_afl.parquet")
