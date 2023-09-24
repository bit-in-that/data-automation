library(arrow)
library(dplyr)
library(reactable)

wafl_data <- read_parquet("state_leagues/data/raw/wafl_player_stats.parquet")
matilda_dyke <- wafl_data |> 
  filter(official_id ==  "CD_I1021409") |> 
  filter(match.season.name >= 2022, match.competition.name == "WAFLW") |> 
  transmute(
    Season = match.season.name,
    hitouts,
    goals,
    behinds,
    disposals,
    tackles,
    fantasy_points = goals * 6 + behinds + kicks * 3 + handballs * 2 + tackles * 4 + frees_for - 3 * frees_against + hitouts,
    match.clubs_vs_string,
    match.round.name,
    match.slug
  ) |>
  group_by(Season) |> 
  summarise(
    Games = n(),
    Average = mean(fantasy_points),
    Min = min(fantasy_points),
    Max = max(fantasy_points)
  )


matilda_dyke |> 
  reactable(
    columns = list(
      Average = colDef(format = colFormat(digits = 1))
    ),
    columnGroups = list(
      colGroup("Fantasy Points", c("Average", "Min", "Max"))
    )
  )

 
