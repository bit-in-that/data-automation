library(httr)
library(arrow)
library(tidyr)
library(dplyr)
library(purrr)
library(stringr)
library(httr)

source("players/modules/get_afl_token.R")
afl_token <- get_afl_token()
match_metadata_afl <- read_parquet("metadata/data/processed/match_metadata_afl.parquet")
teams_metadata_afl <- read_parquet("metadata/data/processed/teams_metadata_afl.parquet")


match_id <- "CD_M20230140103"

headers = c(
  "x-media-mis-token" = afl_token
)

response <- GET(url = paste0("https://api.afl.com.au/cfs/afl/playerStats/match/", match_id), 
                add_headers(headers))
output <- content(response)

output$homeTeamPlayerStats[[1]]$teamId


match_metadata_afl |>
  filter(status == "CONCLUDED")
