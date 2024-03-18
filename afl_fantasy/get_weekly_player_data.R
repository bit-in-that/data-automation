library(dplyr)
library(purrr)
library(arrow)
library(tidyr)

source("_examples/modules/afl_fantasy_apis.R")

# TODO: add "transfers" to this or selections info once it comes through (and quickly, it comes from the palyers2 dataset), can test the code out on the sample from last year
# TODO: also need to add scores and fixture as well, potentially also look up the individual player profiles to see if there is any gold in there too

# List all the data I ignored (some were because they were useless, not yet populated in the preseason, others were because they are covered by selections snapshots):
# - player$original_positions
# - player$stats$career_avg_vs
# - player$stats$selections_info
# - player$stats$prices
# - player$stats$scores
# - player$stats$ranks
# - player2$venues
# - player2$opponents
# - player2$last_season_scores
# - player2$transfers


players <- get_afl_fantasy_players()
players2 <- get_afl_fantasy_players2()
squads <- get_afl_fantasy_squads() 

named_list_to_tibble <- function(x, col_names = c("name", "value")) {
  tibble(
    name = names(x),
    value = unlist(x)
  ) |> 
    `colnames<-`(col_names)
}


tranform_player_df <- function(player) {
  
  positions <- unlist(player$positions)
  
  round_info <- named_list_to_tibble(player$stats$prices, col_names = c("round", "round_price")) |> 
    full_join(
      named_list_to_tibble(player$stats$scores, col_names = c("round", "round_score")),
      by = "round"
    ) |> 
    full_join(
      named_list_to_tibble(player$stats$ranks, col_names = c("round", "round_rank")),
      by = "round"
    )
  
  bind_cols(
    player[c("id", "first_name", "last_name", "slug", "dob", "squad_id", "cost", "status", "is_bye", "locked")],
    list(
      is_defender = 1 %in% positions,
      is_midfielder = 2 %in% positions,
      is_ruck = 3 %in% positions,
      is_forward = 4 %in% positions
    ),
    # rd_tog removed for now, try to bring it back once problem figured out
    player$stats[c("season_rank", "games_played", "total_points", "avg_points", "high_score", "low_score", "last_3_avg", "last_5_avg", "adp", "proj_avg", "tog", "leagues_rostered", "last_3_proj_avg")]
  ) |> 
    mutate(
      position = c(
        {if(is_forward) "Fwd" else NULL},
        {if(is_defender) "Def" else NULL},
        {if(is_ruck) "Ruc" else NULL},
        {if(is_midfielder) "Mid" else NULL}
      ) |> 
        paste(collapse = "/")
    ) |> 
    mutate(
      round_info = list(round_info)
    )
}

tranform_player2_df <- function(player2, player_id) {
  
  future_projections <- named_list_to_tibble(player2$proj_scores, col_names = c("round", "proj_score")) |> 
    full_join(
      named_list_to_tibble(player2$proj_prices, col_names = c("round", "proj_price")),
      by = "round"
    ) |> 
    full_join(
      named_list_to_tibble(player2$break_evens, col_names = c("round", "proj_break_even")),
      by = "round"
    ) |> 
    full_join(
      named_list_to_tibble(player2$be_pct, col_names = c("round", "proj_be_pct")),
      by = "round"
    )
  
  transfers <- player2$transfers |> 
    bind_rows() |> 
    `colnames<-`(c("trades_in", "trades_out")) |> 
    mutate(
      round = names(player2$transfers)
    )
  
  projections_trades_by_round <- future_projections |> 
    full_join(transfers, by = "round")
  
  draft_selections_names <- names(player2$draft_selections_info)
  draft_selections_info <- player2$draft_selections_info |>
    `names<-`(paste0("draft_", draft_selections_names))
  
  
  bind_cols(
    id = player_id,
    player2[c("last_3_proj_avg", "last_3_tog_avg", "consistency", "in_20_avg", "out_20_avg", "draft_selections", "proj_score", "break_even", "last_5_tog_avg")],
    draft_selections_info
  ) |> 
    mutate(
      projections_trades_by_round = list(projections_trades_by_round)
    )
  
}


players_df <- players |> 
  map(tranform_player_df) |> 
  list_rbind()

players2_df <- map2(players2, as.integer(names(players2)), tranform_player2_df) |> 
  list_rbind()

squads_df <- squads |> 
  bind_rows() |> 
  rename_with(~paste0("team_", .x))

af_player_data <- players_df |> 
  left_join(players2_df, by = "id") |> 
  left_join(squads_df, by = c("squad_id" = "team_id"))


af_player_data_by_round <- af_player_data |> 
  select(-proj_score) |> 
  mutate(
    data_by_round = map2(projections_trades_by_round, round_info, full_join, by = "round")
  ) |> 
  select(-projections_trades_by_round, -round_info) |> 
  unnest(data_by_round) |> 
  relocate(round, .before = "id") |> 
  mutate(round = as.integer(round)) |> 
  arrange(id, round)

af_player_data |> 
  # remove this as it is not flat
  select(-projections_trades_by_round, -round_info) |> 
  write_parquet("afl_fantasy/data/processed/2024/af_player_data.parquet")

af_player_data_by_round |> 
  write_parquet("afl_fantasy/data/processed/2024/af_player_data_by_round.parquet")
