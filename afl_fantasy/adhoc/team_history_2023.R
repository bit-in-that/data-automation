library(dplyr)
library(purrr)
library(arrow)
library(jsonlite)
library(tidyr)

af_lineups_data <- read_parquet("afl_fantasy/data/raw/2023/af_lineups_data.parquet")

players <- read_json("_examples/output_2023/players.json",)
squads <- read_json("_examples/output_2023/squads.json")

tranform_player_df <- function(player) {
  
  prices <- player$stats$prices
  
  prices_df <- tibble(
    round = names(prices),
    price = unlist(prices)
  )
  
  scores <- player$stats$scores
  scores_df <- tibble(
    round = names(scores),
    score = unlist(scores)
  )
  
  round_info_df <- prices_df |>
    left_join(scores_df, by = "round")
  
  positions <- unlist(player$positions)
  
  bind_cols(
    list(
      id = player$id[[1]],
      first_name = player$first_name[[1]],
      last_name = player$last_name[[1]],
      squad_id = player$squad_id[[1]]
    ),
    list(
      is_defender = 1L %in% positions,
      is_midfielder = 2L %in% positions,
      is_ruck = 3L %in% positions,
      is_forward = 4L %in% positions
    )
  ) |> 
    mutate(
      round_info_df = list(round_info_df),
      position = c(
        {if(is_forward) "Fwd" else NULL},
        {if(is_defender) "Def" else NULL},
        {if(is_ruck) "Ruc" else NULL},
        {if(is_midfielder) "Mid" else NULL}
      ) |> 
        paste(collapse = "/")
    )
}


players_df <- players |> 
  map(tranform_player_df) |> 
  list_rbind() |>
  mutate(
    player_name = paste(first_name, last_name)
  )


squads_df <- squads |> 
  bind_rows() |> 
  mutate(across(everything(), unlist)) |>
  rename_with(~paste0("team_", .x))




af_player_data <- players_df |> 
  left_join(squads_df, by = c("squad_id" = "team_id")) |>
  unnest(round_info_df) |>
  select(-team_name, -first_name, -last_name) |> 
  rename(player_score = score)



af_lineups_data |> 
  distinct(team_id, round) |> 
  mutate(
    trades = map2(as.integer(round), team_id, ~ {
      
      before_ids <- af_lineups_data |> 
        filter(round == (.x - 1), team_id == .y) |>
        pull(player_id)
      
      after_ids <- af_lineups_data |> 
        filter(round == .x, team_id == .y) |> 
        pull(player_id)
      
      list(
        trades_in = setdiff(after_ids, before_ids),
        trades_out = setdiff(after_ids, before_ids)
      ) |> 
        bind_cols()
    })
  ) |> 
  head() |> 
  pull(trades)


af_lineups_data |>
  left_join(af_player_data, by = c("player_id" = "id", "round")
    ) |>
  data.table::fwrite("../random/test.csv")


