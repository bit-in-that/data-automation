library(arrow)
source("state_leagues/modules/get_sanfl_data.R")

sanfl_ladder_history <- get_sanfl_ladder_history()

sanfl_club_slug_map <- c(
  "ADL" = "adelaide",
  "CNTRL" = "central-district",
  "TIGERS" = "glenelg",
  "NORTH" = "north-adelaide",
  "NWD" = "norwood",
  "PORT" = "port-adelaide",
  "SOUTH" = "south-adelaide",
  "STURT" = "sturt",
  "WEST" = "west-adelaide",
  "EAGLES" = "woodville-west-torrens"
)

sanfl_team_map <- sanfl_ladder_history |> 
  group_by(comp, squadId, teamCode, squadName) |>
  count() |>
  ungroup() |>
  arrange(desc(n)) |>
  select(-n) |>
  distinct(comp, squadId, .keep_all = TRUE) |>
  arrange(comp, squadId) |>
  mutate(
    club_slug = sanfl_club_slug_map[teamCode]
  ) |> 
  add_row(
    comp = "state", squadId = "1528", teamCode = "SANFL", squadName = "Croweaters", club_slug = NA_character_
  ) |> 
  add_row(
    comp = "state", squadId = "1528", teamCode = "WAFL", squadName = "Sandgropers", club_slug = NA_character_
  )

write_parquet(sanfl_ladder_history, "state_leagues/data/raw/sanfl_ladder_history.parquet")
write_parquet(sanfl_team_map, "state_leagues/data/raw/sanfl_team_map.parquet")
