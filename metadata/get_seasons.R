library(dplyr)
library(purrr)
library(stringr)
library(httr)
library(arrow)

competition_metadata_afl <- read_parquet("metadata/data/processed/competition_metadata_afl.parquet")

get_seasons <- function(season_id) {
  response <- GET(paste0("https://aflapi.afl.com.au/afl/v2/competitions/", season_id, "/compseasons?pageSize=100"))
  content(response)
} 

season_metadata_afl <- competition_metadata_afl$id |> 
  map(~{
    get_seasons(.x)$compSeasons |> 
      bind_rows() |> 
      mutate(competition_id = .x)
  }) |> 
  list_rbind() |> 
  mutate(
    year = case_when(
      is.na(name) ~ str_extract(providerId, "\\d{4}"),
      str_detect(name, "\\d{4}") ~ str_extract(name, "\\d{4}"),
      TRUE ~ str_extract(providerId, "\\d{4}")
    ) |> 
      as.integer()
  )

afl_comp_code <- competition_metadata_afl |> 
  filter(name_lookup == "AFL") |> 
  pull(comp_code)
competition_id <- competition_metadata_afl |> 
  filter(name_lookup == "AFL") |> 
  pull(id)

season_metadata_afl_extra <- 
  tibble(
    year = 2001L:2011L,
    currentRoundNumber = c(rep(26L, 10L), 28L), # 28 had an odd number of teams (i.e. byes  ) so had 24 rounds in the H&A season
    name = paste0(year, " Toyota AFL Premiership"),
    shortName = "Premiership",
    providerId =paste0("CD_S", year, afl_comp_code),
    competition_id = competition_id
)

season_metadata_afl_extended <- bind_rows(season_metadata_afl, season_metadata_afl_extra)

write_parquet(season_metadata_afl, "metadata/data/processed/season_metadata_afl.parquet")
write_parquet(season_metadata_afl_extended, "metadata/data/processed/season_metadata_afl_extended.parquet")

