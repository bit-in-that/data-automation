library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(arrow)


get_teams_list <- function(page_index, pageSize = 100) { # 100 is the max page size (two pages of teams)
  response <- GET(paste0("https://aflapi.afl.com.au/afl/v2/teams?pageSize=", pageSize, "&page=", page_index))
  output <- content(response)
}

get_clubs_list <- function(page_index, pageSize = 100) { 
  response <- GET(paste0("https://aflapi.afl.com.au/afl/v2/clubs?pageSize=", pageSize, "&page=", page_index))
  output <- content(response)
}

## team data ---
teams_list_0 <- get_teams_list(page_index = 0, pageSize = 100)
teams_list <- teams_list_0$teams

numPages <- teams_list_0$meta$pagination$numPages
pageSize <- teams_list_0$meta$pagination$pageSize

if(numPages > 1) {
  for(page_index in 1:(numPages-1)) {
    teams_list_n <- get_teams_list(page_index, pageSize)
    teams_list <- c(teams_list, teams_list_n$teams)
  }
}
## club data ---
clubs_list_0 <- get_clubs_list(page_index = 0, pageSize = 100)
clubs_list <- clubs_list_0$clubs

numPages_club <- clubs_list_0$meta$pagination$numPages
pageSize_club <- clubs_list_0$meta$pagination$pageSize

if(numPages_club > 1) {
  for(page_index in 1:(numPages_club-1)) {
    teams_list_n <- get_teams_list(page_index, pageSize_club)
    teams_list <- c(teams_list, teams_list_n$teams)
  }
}

# convert the data to a flat tibble  ----
# (the current method seems a bit inefficient due to converting list columns)

teams_metadata_afl <- teams_list |> 
  toJSON() |> 
  fromJSON(flatten = TRUE) |> 
  as_tibble() |> 
  mutate(across(everything(), ~ {
    map_chr(.x, \(y) {
      if(is.null(y)) {
        NA_character_
      } else {
        as.character(y)
      }
      
    })
  })) |> 
  mutate(
    id = as.integer(id),
    club.id = as.integer(club.id)
  )

clubs_metadata_afl <- clubs_list |> 
  toJSON() |> 
  fromJSON(flatten = TRUE) |> 
  as_tibble() |>
  mutate(across(everything(), ~ {
    map_chr(.x, \(y) {
      if(is.null(y)) {
        NA_character_
      } else {
        as.character(y)
      }
      
    })
  })) |> 
  mutate(
    id = as.integer(id)
  )

# save data ----

saveRDS(teams_list, "metadata/data/raw/teams_metadata_list_afl.RDS")
saveRDS(clubs_list, "metadata/data/raw/clubs_metadata_list_afl.RDS")

write_parquet(clubs_metadata_afl, "metadata/data/processed/clubs_metadata_afl.parquet")
write_parquet(teams_metadata_afl, "metadata/data/processed/teams_metadata_afl.parquet")

