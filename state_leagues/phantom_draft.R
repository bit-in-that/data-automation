library(rvest)
library(dplyr)
library(stringr)
library(tidyr)

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

tibble(
  ranking = 1L:30L,
  afl = phantom_draft_order_afl,
  sporting_news = phantom_draft_order_sporting_news,
  fox_sports = phantom_draft_order_fox_sports,
  abc = phantom_draft_order_abc
) |>
  pivot_longer(
    
  )

phantom_draft_articles <- tibble(
  article_url = c(afl_url, sporting_news_url, fox_sports_url, abc_url),
  article_date = as.Date(c("2023-09-12", "2023-10-30", "2023-10-16", "2023-11-05"))
)


