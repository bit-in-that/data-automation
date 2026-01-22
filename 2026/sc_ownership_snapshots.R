library(dplyr)
library(purrr)
library(arrow)
library(httr2)


# player_selections_initial <- read_parquet("2026/output/player_selections.parquet")

players_url <- "https://www.supercoach.com.au/2026/api/afl/classic/v1/players-cf?year=2026&round=1&embed=player_stats"

players <- request(players_url) |> 
  req_perform() |> 
  resp_body_json()


player_selections <- players |> 
  map(~{
    tibble(
      id = .x$id,
      first_name = .x$first_name,
      last_name = .x$last_name,
      feed_id = .x$feed_id,
      team = .x$team$abbrev,
      team_name = .x$team$name,
      price = .x$price,
    owned = if(length(.x$player_stats) > 0) .x$player_stats[[1]]$owned else NA_real_,
    own_raw = if(length(.x$player_stats) > 0) .x$player_stats[[1]]$own_raw else NA_real_,
    price = if(length(.x$player_stats) > 0) .x$player_stats[[1]]$price else NA_real_
    )
  }) |> 
  bind_rows() |> 
  mutate(
    snapshot_time = Sys.time()
  )

player_selections_minimal <- player_selections |> 
  select(id, owned, owned_raw, snapshot_time)

#player_selections_initial |> 
  bind_rows(player_selections_minimal) |> 
  write_parquet("2026/output/sc_player_selections.parquet")

if(FALSE) { # As to not be run by action:
  #TODO: adjust this for SC
  player_selections |> 
    mutate(
      ownership_adjusted = round(ownership / sum(ownership) * 3000, 2)
    ) |> 
    clipr::write_clip()
}