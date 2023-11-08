# this script scrapes the rookieme website
library(rvest)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(arrow)

# other data
venues_metadata_afl <- read_parquet("metadata/data/processed/venues_metadata_afl.parquet")

# functions ----
get_u18_champs_matches <- function(u18_champs_state_url) {
  u18_champs_state_page <- u18_champs_state_url |>
    read_html()
  
  time_elements <- u18_champs_state_page |>
    rvest::html_element(".sp-fixtures-results") |>
    rvest::html_elements("time a")
  
  match_url <- time_elements |>
    html_attr("href")
    
  match_date <- time_elements |>
    html_text() |> 
    as.Date(format = "%B %e, %Y")
  
  tibble(
    match_url,
    match_date
  )
  
}

get_u18_champs_player_stats <- function(match_url) {
  match_page <- match_url |> 
    read_html()
  
  details_table <- match_page |> 
    html_elements(".sp-template-event-details") |> 
    html_table() |> 
    pluck(1)
  
  results_table <- match_page |> 
    html_elements(".sp-template-event-results")
  
  if(length(results_table) == 0L) {
    return(NULL)
  }
  
  results_table_headers <- results_table |> 
    html_elements("th") |> 
    html_text()
  
  results <- results_table |> 
    html_table() |> 
    pluck(1)
  
  results <- bind_cols(
    results |> slice(1) |> `names<-`(paste0("home_", results_table_headers)),
    results |> slice(2) |> `names<-`(paste0("away_", results_table_headers))
  )
  
  
  player_stats_tables <- match_page |> 
    html_elements(".sp-template-event-performance-values")
  
  player_stats <- player_stats_tables |> 
    html_table()
  
  df_has_players <- map_lgl(player_stats, ~ "Player" %in% names(.x))
  
  player_stats <- player_stats[df_has_players]
  
  if(length(player_stats) == 0L) {
    return(NULL)
  }
  
  team_name <- player_stats_tables |> 
    html_elements(".sp-table-caption") |> 
    html_text()
  
  team_name <- team_name[df_has_players]
  
  player_url <- player_stats_tables |> 
    html_elements(".data-name a") |> 
    html_attr("href")
  
  venue <- match_page |> 
    html_element(".sp-event-venue thead th") |> 
    html_text()
  
  venue_address <- match_page |> 
    html_element(".sp-event-venue-address-row") |> 
    html_text() |> 
    str_remove("\\s+$")
  
  tibble(
    team_name,
    venue,
    venue_address,
    player_stats
  ) |> 
    bind_cols(results) |> 
    bind_cols(details_table) |> 
    mutate(
      player_stats = map(player_stats, ~{
        .x |> 
          mutate(
            `#` = as.character(`#`) # some weird characters messing with the numbers, e.g. https:\\central.rookieme.com\afl\event\vic-country-girls-vs-vic-metro-girls-3\ 
          )
      })
    ) |> 
    unnest(player_stats) |> 
    filter(Player != "Total") |> 
    mutate(player_url) |> 
    rename(player_number = `#`)
}

state_slugs_boys <- c("south-australia", "vic-country", "vic-metro", "western-australia", "allies-2")
state_slugs_girls <- head(state_slugs_boys, -1) |> 
  paste0("-girls") |> 
  c("allies-girls")

state_slugs <- c(state_slugs_boys, state_slugs_girls)

u18_champs_matches <- paste0("https://central.rookieme.com/afl/team/", state_slugs) |> 
  map(get_u18_champs_matches) |> 
  bind_rows() |> 
  distinct(match_url, .keep_all = TRUE)

system.time({ # takes about a minute (and a half now with the girls stuff)
  u18_champs_player_stats_initial <- u18_champs_matches |> 
    mutate(
      player_stats = map(match_url, get_u18_champs_player_stats)
    )
})


u18_champs_player_stats_intermediate <- u18_champs_player_stats_initial |> 
  unnest(player_stats) |>
  mutate(
    venue_lowercase = case_when(
      venue == "Football Park" ~ "AAMI Stadium",
      venue == "Melbourne Cricket Ground" ~ "MCG",
      venue == "The Gabba" ~ "Gabba",
      venue == "Utas Stadium (Launceston)" ~ "Utas Stadium",
      venue == "Blacktown International Sportspark" ~ "Blacktown ISP",
      venue == "Swinburne Centre" ~ "Punt Road Oval",
      venue == "Brighton Holmes Arena" ~ "Brighton Homes Arena",
      venue == "Southport" ~ "Fankhauser Reserve",
      venue == "Trevor Barker Oval" ~ "Trevor Barker Beach Oval",
      venue == "Metricon Stadium" ~ "Heritage Bank Stadium",
      TRUE ~ venue 
      ) |> 
      str_remove_all(" \\(.*\\)") |> 
      str_to_lower()
  ) |> 
  left_join(
    venues_metadata_afl |> transmute(venue_lowercase = str_to_lower(name), timezone),
    by = "venue_lowercase"
  ) |> 
  filter(
    (K + HB + D +  M + CP + UP + T + HO + CLR + I50 + R50 + GL) > 0
  ) |> 
  mutate(
    timezone = case_when(
      !is.na(timezone) ~ timezone,
      str_detect(home_Team, "^South Australia") ~ "Australia/Adelaide",
      TRUE ~ NA_character_
      )
  )

# check this is empty
u18_champs_player_stats_intermediate |> filter(is.na(timezone)) |> View()
u18_champs_player_stats_intermediate |> filter(is.na(timezone)) |> pull(venue) |> unique()

u18_champs_player_stats <- u18_champs_player_stats_intermediate |> 
  distinct(Date, Time, timezone) |>
  mutate( # fix the timezone
    match_time = map2_vec(paste(Date, Time), timezone, ~as.POSIXct(.x, format = "%B %e, %Y %I:%M %p", tz = .y))
  ) |> 
  left_join(u18_champs_player_stats_intermediate, y = _, by = c("Date", "Time", "timezone"))

write_parquet(u18_champs_matches, "state_leagues/data/raw/u18_champs_matches.parquet")
write_parquet(u18_champs_player_stats, "state_leagues/data/raw/u18_champs_player_stats.parquet")
