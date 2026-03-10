# try to get snapshots of player selections with time stamps
library(dplyr)
library(purrr)
library(arrow)
library(httr2)
library(tidyr)


MAGIC_NUMBER_PRESEASON <- 10490
BASEMENT_PRICE <- 230000
PRICING_CHANGE_COMMON_FACTOR <- 0.25
SCORE_HISTORY_COUNT_CAP <- 5L
ROUNDING_PRECISION <- -3 # thousands


pricing_parameters <- tibble(score_count = SCORE_HISTORY_COUNT_CAP:1L) |> 
  mutate(
    # score_count = rev(k),
    weight = score_count / sum(score_count),
    weights = map(score_count, ~head(weight, n = .x)),
    price_change_factor = PRICING_CHANGE_COMMON_FACTOR * map_dbl(weights, sum),
    weights_breakeven = map(weights, tail, n = -1),
    breakeven_factor = map_dbl(weights_breakeven, sum) / head(weight, 1)
  )

calculate_price_change <- function(scores, price, magic_number) {
  scores_trunc <- scores |> 
    na.omit() |>
    tail(n = SCORE_HISTORY_COUNT_CAP) |> 
    rev() # reverse as this assumes scores are in chronological order
  
  score_count_trunc <- length(scores_trunc)
  parameters <- pricing_parameters |> 
    filter(score_count == score_count_trunc)
  
  price_change_factor <- parameters$price_change_factor
  previous_priced_at <- price / magic_number
  weighted_average <- weighted.mean(scores_trunc, parameters$weights[[1]])
  priced_at_change_raw <- price_change_factor * (weighted_average - previous_priced_at)
  priced_at_new_raw <- previous_priced_at + priced_at_change_raw
  price_change_raw <- magic_number * priced_at_change_raw
  
  # Avoid going below basement:
  if(price + price_change_raw <= BASEMENT_PRICE) {
    price_new_raw <- BASEMENT_PRICE
    price_change_raw <- price_new_raw - price
  } else {
    price_new_raw <- max(price + price_change_raw, BASEMENT_PRICE)
  }
  
  # The below not outputed because the approach to rounding needs to context of all other prices to be done properly (retain the sum of changes being 0)
  price_new_rounded <- round(price_new_raw, ROUNDING_PRECISION)
  price_change_rounded <- price_new_rounded - price
  
  tibble(
    weighted_average,
    priced_at_new_raw,
    priced_at_change_raw,
    price_change_raw,
    price_new_raw,
    price_change_rounded,
    price_new_rounded
    )
}

calculate_breakeven <- function(scores, price, magic_number, price_target = price) {
  scores_trunc <- scores |> 
    na.omit() |>
    tail(n = SCORE_HISTORY_COUNT_CAP - 1) |> 
    rev() # reverse as this assumes scores are in chronological order
  
  score_count_trunc <- length(scores_trunc)
  
  parameters <- pricing_parameters |> 
    filter(score_count == score_count_trunc + 1)
  
  first_weight <- parameters$weights[[1]] |> 
    head(n = 1)
  weights_breakeven <- parameters$weights_breakeven[[1]]
  total_weights <- sum(weights_breakeven) + first_weight
  
  priced_at <- price / magic_number
  priced_at_target <- price_target / magic_number
  
  price_difference_term <- (priced_at_target - priced_at) / PRICING_CHANGE_COMMON_FACTOR
  
  if(length(scores_trunc) == 0) {
    weighted_average_previous <- 0
    
  } else {
    weighted_average_previous <- weighted.mean(scores_trunc, weights_breakeven) 
    
  }
  weighted_sum_previous <- weighted_average_previous  * sum(weights_breakeven)
  
  breakeven_raw <- (price_difference_term + total_weights * priced_at - weighted_sum_previous) / first_weight
  
  breakeven <- ceiling(breakeven_raw)
  tibble(
    weighted_average_previous = weighted_average_previous,
    next_score_weight = first_weight / total_weights,
    # breakeven_average_vs_price = breakeven_average_vs_price,
    breakeven_raw = breakeven_raw,
    breakeven = breakeven,
  )
}

calculate_price_change_single <- function(..., output_column = "price_change_rounded") {
  calculate_price_change(...)[[output_column]]
}

calculate_breakeven_single <- function(..., output_column = "breakeven") {
  calculate_breakeven(...)[[output_column]]
}

round_preserve_sum <- function(x, digits = 0, target = NA_real_) {
  significance <- 10^(-digits)
  floored <- trunc(x / significance) * significance
  remainders <- abs(x - floored)
  
  if (is.na(target)) {
    target <- sum(x)
  }
  
  shortfall <- round(target / significance) * significance - sum(floored)
  n <- round(shortfall / significance)
  
  if (abs(n) > length(x)) {
    stop(sprintf(
      "Target %.10g is not achievable by rounding: shortfall of %g requires %d adjustments but only %d elements exist.",
      target, shortfall, abs(n), length(x)
    ))
  }
  
  step <- sign(n) * significance
  indices <- order(remainders, decreasing = TRUE)
  adjust_idx <- indices[seq_len(abs(n))]
  floored[adjust_idx] <- floored[adjust_idx] + step
  floored
}

player_selections_initial <- read_parquet("2026/output/player_selections.parquet")

players_url <- "https://fantasy.afl.com.au/json/fantasy/players.json"
players_coach_url <- "https://fantasy.afl.com.au/json/fantasy/coach/players.json"

players <- request(players_url) |> 
  req_perform() |> 
  resp_body_json()

players_coach <- request(players_coach_url) |> 
  req_perform() |> 
  resp_body_json()


af_official_projections <- players_coach |> 
  map(~{
    tibble(
      id = .x$id,
      projectedScores = list(.x$projectedScores),
      breakeven = list(.x$breakeven),
      projectedPriceChange = list(.x$projectedPriceChange),
      breakevenPctChance = list(.x$breakevenPctChance),
    )
  }) |> 
  list_rbind()

players_df <- players |> 
  map(~{
    r0_score <- .x$scores[["1"]]
    if(is.null(r0_score)) {
      r0_score <- NA_integer_
    }
    
    tibble(
      id = .x$id,
      first_name = .x$firstName,
      last_name = .x$lastName,
      price = .x$price,
      r0_score = r0_score,
      position = paste(unlist(.x$position), collapse = "/"),
      price_per_point = .x$pricePerPoint
    )
  }) |> 
  list_rbind() |> 
  mutate(
    player_name = paste(first_name, last_name)
  )


magic_number_working <- players_df |> 
  filter(!is.na(r0_score)) |> 
  summarise(
    total_points = sum(r0_score),
    total_price = sum(price),
    average_points = mean(r0_score),
    average_price = mean(price)
  ) |> 
  mutate(
    magic_number = total_price / total_points
  )

magic_number_r0 <- magic_number_working |> 
  pull(magic_number)
# TODO: more analysis to get a clearer picture of R1 magic number:
# if we know who is playing we can get the sum of prices, so only thing left is average points 
#  - can do a Bayesian approach perhaps to figure out the average points for the next game?
#    - assume some of the uplift in average scoring is real? but only partially realise, account for the teams playing as well
#    - is the average total score in games that involve a team fairly stable?
#    -  linear model with factor variable of teams, see if any teams are statistically significant on the total points scored in a game
#       - compare to average points involving them
#       - also put venue as a factor in there 
#    - probably a more statistically rigorous way to do this kind of analysis (facotr analysis perhaps?)

magic_number_r1 <- magic_number_r0



r0_player_prices <- players_df |> 
  filter(!is.na(r0_score)) |> 
  mutate(
    price_change_data = map2(r0_score, price, calculate_price_change, magic_number = magic_number_r0)
  ) |> 
  unnest(price_change_data) |> 
  mutate(
    price_change = round_preserve_sum(price_change_raw, digits = -3, target = 0),
    price_new = price + price_change
  ) |> 
  mutate(
    breakeven_data = pmap(list(r0_score, price_new, price), calculate_breakeven, magic_number = magic_number_r1)
  ) |> 
  unnest(breakeven_data) |> 
  mutate(
    breakeven_data_hidden = map2(r0_score, price_new, calculate_breakeven, magic_number = magic_number_r1) |> 
      map(rename_with, .fn = ~paste0(.x, "_hidden"))
  ) |> 
  unnest(breakeven_data_hidden)

# check that the prices changes sum to 0:
stopifnot(sum(r0_player_prices$price_change) == 0)
stopifnot(min(r0_player_prices$price_new) >= BASEMENT_PRICE)

afl_fantasy_r0_breakevens <- r0_player_prices |> 
  select(
    id,
    player_name,
    position,
    r0_score,
    starting_price = price,
    hidden_price = price_new,
    breakeven = breakeven,
    breakeven_to_hit_hidden_price = breakeven_hidden
  ) |> 
  arrange(desc(r0_score))

afl_fantasy_r0_breakevens |> 
  data.table::fwrite("2026/output/afl_fantasy_r0_breakevens.csv")

vectorised_price_change <- function(next_score, previous_scores, price, magic_number) {
  map2(previous_scores, next_score, c) |> 
    map2_int(price, .f = calculate_price_change_single, magic_number = magic_number)
  
}

vectorised_breakeven <- function(previous_scores, price_target, price, magic_number) {
  pmap_int(list(previous_scores, price, price_target), .f = calculate_breakeven_single, magic_number = magic_number)
  # scores, price, magic_number, price_target = price
}


r1_projected_price_changes <- players_df |> 
  left_join(
    r0_player_prices |> 
      mutate(
        r1_proj_change_r0 = vectorised_price_change(next_score = r0_score, previous_scores = r0_score, price = price_new, magic_number = magic_number_r1) + price_change
      ) |> 
      select(id, price_hidden = price_new, price_change_hidden = price_change, breakeven_hidden, r1_proj_change_r0),
    by = "id"
  ) |> 
  mutate(
    played_r0 = is.na(r0_score),
    price_r0 = coalesce(price_hidden, price),
    price_change_r0 = coalesce(price_change_hidden, 0)
  ) |> 
  reduce(
    .init = _,
    .x = 10 * (0:15),
    \(x, score){
      x |> 
        mutate(
          "r1_proj_change_{score}" := vectorised_price_change(next_score = score, previous_scores = r0_score, price = price_r0, magic_number = magic_number_r1) + price_change_r0
        )
    }
  ) |> 
  mutate(
    priced_at_preseason = round(price / MAGIC_NUMBER_PRESEASON, 0),
    r1_proj_change_priced_at = vectorised_price_change(next_score = priced_at_preseason, previous_scores = r0_score, price = price_r0, magic_number = magic_number_r1) + price_change_r0,
  ) |> 
  reduce(
    .init = _,
    .x = 25 * (-6:6),
    \(x, change){
      change_label <- if_else(change < 0, paste0("neg", abs(change)), as.character(change))
      target_change <- change * 1000
      x |> 
        mutate(
          " r1_proj_breakeven_{change_label}" := vectorised_breakeven(price = price_r0, price_target = price + target_change, previous_scores = r0_score, magic_number = magic_number_r1)
        )
    }
  )

r1_projected_price_changes |> 
  write_parquet("2026/output/r1_projected_price_changes.parquest")

# TODO: some projections on what happens if a player scores x
# Jagger BE Check:
calculate_price_change(scores = c(82, -75), price = 280000, magic_number = magic_number_r1)
calculate_price_change(scores = c(82, 82), price = 280000, magic_number = magic_number_r1)

calculate_price_change(scores = c(82, 82, 82), price = 363213, magic_number = magic_number_r1)
calculate_price_change(scores = c(82, 82, 82, 82), price = 457520, magic_number = magic_number_r1)
calculate_price_change(scores = c(82, 82, 82, 82, 82), price = 545540, magic_number = magic_number_r1)

calculate_price_change(scores = c(94, -22), price = 543000, magic_number = magic_number_r1)
calculate_price_change(scores = c(98, -27), price = 557000, magic_number = magic_number_r1)


calculate_price_change(scores = c(82, 80), price = 280000, magic_number = magic_number_r1)
calculate_price_change(scores = c(20, 174), price = 1072000, magic_number = magic_number_r0)
# Fonti Sam Taylor check
calculate_price_change(scores = c(90, 47), price = 554000, magic_number = magic_number_r1)
calculate_price_change(scores = c(90), price = 514000, magic_number = magic_number_r1)
calculate_price_change(scores = c(118), price = 51*MAGIC_NUMBER_PRESEASON, magic_number = magic_number_r1)
calculate_price_change(scores = c(118, 47), price = 590510, magic_number = magic_number_r1)
621886 - 51*MAGIC_NUMBER_PRESEASON

calculate_price_change(scores = c(109, 60), price = 605000, magic_number = magic_number_r1)$price_change_raw +605000- 559000
calculate_price_change(scores = c(61, 55), price = 262000, magic_number = magic_number_r1)$price_change_raw +262000- 230000



calculate_breakeven(scores = 82, price = 280000, magic_number = magic_number_r1)
calculate_breakeven(scores = c(82, 82), price = 280000, magic_number = magic_number_r1)
calculate_breakeven(scores = c(82, 82, 82), price = 280000, magic_number = magic_number_r1)


calculate_breakeven(scores = 82, price = 230000, magic_number = magic_number_r1)
calculate_breakeven(scores = 82, price = 280000, magic_number = magic_number_r1, price_target = 230000)
calculate_breakeven(scores = 20, price = 1000000, magic_number = magic_number_r1, price_target = 1072000)


# TODO: cross validate breakevens and projections with official ones



# TODO: page that summarises the following:
# have a caveat at the top about it differing from official prices possibly and that the magic number is unknown which is part of the breakeven and price change formula
# - table of R0 players and what they scored, hidden price and breakeven to hit hidden price, hidden price change
# - R1 price change for all players if they score 0, 25, 50, 75, 100, 125, 150 (could make increments smaller potentially like increments of 5 but might not diplay well in the table) -> could have tabs for the more granular tables (e.g. increments of 5 from 0 - 25, etc)
# - R1 score require to make x cash -100k, 50k, 0k, 50k, 100k (increments of 25 or 50k depending what looks best) also to hit hidden price

# also table that show price change over next x week if they go at their starting priced at, r0 score


# fully flxible price projector a bit tricky (will require javascript to do)
# also thought of having charts for each player plotting price against score with horizontal lines for current and hidden price (intersection represents break-even) - could be price change charts instead or price change in hover metadata

# will need to build jsons to make interactive which might be annoying



# indicate opening round players