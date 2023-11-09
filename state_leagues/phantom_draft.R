library(rvest)
library(dplyr)
library(stringr)
library(purrr)
library(arrow)

combine_data_ids <- read_parquet("players/data/raw/combine_data_ids.parquet")

afl_url <- "https://www.afl.com.au/news/1030664/cal-twomeys-phantom-form-guide-top-draft-prospects-september-ranking/amp"
sporting_news_url <- "https://www.sportingnews.com/au/afl/news/phantom-draft-2023-first-round-projection-top-20-picks/f7057d7638129eb2395ecbd8"
fox_sports_url <- "https://www.foxsports.com.au/afl/draft/afl-draft-2023-power-rankings-afl-draft-news-rankings-after-2023-combine-date-picks-order/news-story/1169252a1e73bbf3d37d5dea0c5f3467"
abc_url <- "https://amp.abc.net.au/article/103039892"

phantom_draft_order_afl <- c(
  "Harley Reid",
  "Jed Walter",
  "Zane Duursma",
  "Colby McKercher",
  "Nick Watson",
  "Daniel Curtin",
  "Ryley Sanders",
  "Nate Caddy",
  "Ethan Read",
  "Connor O'Sullivan",
  "Jordan Croft",
  "Jake Rogers",
  "Ollie Murphy",
  "Caleb Windsor",
  "Darcy Wilson",
  "James Leake",
  "Riley Hardeman",
  "Will McCabe",
  "Koltyn Tholstrup",
  "Harry DeMattia",
  "Archer Reid",
  "Archie Roberts",
  "Will Green",
  "Lance Collard",
  "Tew Jiath",
  "Charlie Edwards",
  "Mitch Edwards",
  "George Stevens",
  "Jack Delean",
  "Ashton Moir"
) |> 
  str_to_upper()

phantom_draft_order_sporting_news <- sporting_news_url|> 
  read_html() |> 
  html_elements("h3") |> 
  html_text() |> 
  str_subset("No\\.") |> 
  str_remove(" \\-.*") |> 
  str_remove("^No\\.[0-9]{1,2} ") |> 
  str_replace("’", "'") |> 
  str_replace("De Mattia", "DeMattia") |> 
  str_to_upper()

phantom_draft_order_fox_sports <- fox_sports_url |> 
  read_html() |> 
  html_elements("b") |> 
  html_text() |> 
  str_subset("[0-9]{1,2}\\.") |> 
  str_remove("^[0-9]{1,2}\\. ") |> 
  str_replace("’", "'") |> 
  str_trim() |> 
  rev() |> 
  str_to_upper()

phantom_draft_order_abc <- abc_url |> 
  read_html() |> 
  html_elements("h2.SYcM3") |> 
  html_text() |> 
  str_remove("^[0-9]{1,2}\\.\\s") |> 
  str_to_upper()

phantom_draft_rankings_initial <- tibble(
  ranking = 1L:30L,
  afl = phantom_draft_order_afl,
  sporting_news = phantom_draft_order_sporting_news,
  fox_sports = phantom_draft_order_fox_sports,
  abc = phantom_draft_order_abc
)


get_player_rankings <- function(player_name, col_name) {
  out <- which(phantom_draft_rankings_initial[[col_name]] == player_name)
  if(length(out) == 1L) {
    out
  } else {
    NA_integer_
  }
}

phantom_draft_rankings_intermediate <- phantom_draft_rankings_initial |> 
  select(afl:abc) |> 
  unlist() |> 
  unique() |> 
  tibble(
    player_name_upper = _
  ) |> 
  mutate(
    afl = map_int(player_name_upper, get_player_rankings, col_name = "afl"),
    sporting_news = map_int(player_name_upper, get_player_rankings, col_name = "sporting_news"),
    fox_sports = map_int(player_name_upper, get_player_rankings, col_name = "fox_sports"),
    abc = map_int(player_name_upper, get_player_rankings, col_name = "abc"),
    player_name_upper = player_name_upper |> 
      str_replace("^OLLIE\\b", "OLIVER") |> 
      str_replace("^WILL\\b", "WILLIAM") |> 
      str_replace("^MITCH\\b", "MITCHELL") 
  )

phantom_draft_articles <- tibble(
  article_url = c(afl_url, sporting_news_url, fox_sports_url, abc_url),
  article_date = as.Date(c("2023-09-12", "2023-10-30", "2023-10-16", "2023-11-05"))
)

# check if the player id gets mapped on correctly for all players:
phantom_draft_rankings <- combine_data_ids |> 
  transmute(
    player_name_upper = str_to_upper(paste(NAME, SURNAME)), playerId
  ) |> 
  right_join(
    phantom_draft_rankings_intermediate, by = "player_name_upper"
  )

write_parquet(phantom_draft_rankings, "state_leagues/data/raw/phantom_draft_rankings.parquet")
write_parquet(phantom_draft_articles, "state_leagues/data/raw/phantom_draft_articles.parquet")
