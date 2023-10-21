# packages ----
library(pdftools)
library(rvest)
library(purrr)
library(dplyr)
library(stringr)
library(tidyr)
library(arrow)

# paths ----
draft_combine_url <- "https://www.afl.com.au/news/993140/2023-afl-draft-combine-list-announced"
state_combine_url <- "https://www.afl.com.au/news/1015904/2023-afl-state-draft-combines-list-announced"
u16_squads_url <- "https://resources.afl.com.au/afl/document/2023/06/23/1900d7ac-c423-4bc4-b3a6-d182be805148/2023-AFL-National-Development-Championships-U16-Boys-Squads.pdf"
u18_squads_url <- "https://resources.afl.com.au/afl/document/2023/07/13/ba2ac438-ada3-4bc7-a544-66683fc395a9/Squads-2023-AFL-National-Championships-U18-Boys.pdf"
# draft_combine_url <- "../_sites/2023 AFL Draft Combine list announced.html"
# state_combine_url <- "../_sites/2023 AFL State Draft Combines list announced.html"
# u16_squads_url <- "../2023-AFL-National-Development-Championships-U16-Boys-Squads.pdf"
# u18_squads_url <- "../Squads-2023-AFL-National-Championships-U18-Boys.pdf"

# functions ----
get_headers <- function(pdf_lines, header_line) {
  pdf_lines[header_line] |>
    str_split("\\s{2,}") |>   
    pluck(1)
  
}

get_pdf_table <- function(lines, column_headers) {
  # lines is just the lines corresponding to the body of the table
  column_count <- length(column_headers)
  
  line_width <- lines |> 
    str_length() |> 
    max()
  
  lines_complete_only <- lines[str_count(lines, "\\b(\\S+ ?)+\\b") == column_count]
  
  gap_overlaps <- lines_complete_only |> 
    str_locate_all("\\s{2,}") |> 
    imap(~ tibble(table_row = .y) |> bind_cols(.x) |> mutate(gap_index = seq_along(table_row))) |> 
    bind_rows() |> 
    mutate(
      character_range = map2(start, end, seq)
    ) |> 
    group_by(gap_index) |> 
    summarise(
      overlap  = list(reduce(character_range, intersect)),
      .groups = "drop"
    ) |> 
    transmute(
      overlap_min = map_int(overlap, min),
      overlap_max = map_int(overlap, max)
    )
  
  column_start_end <- tibble(
    column_headers
  ) |> 
    mutate(
      start = c(1, gap_overlaps$overlap_min),
      end = c(gap_overlaps$overlap_max - 1, line_width),
    )
  table_initial <- column_start_end |> 
    mutate(
      columns = map2(start, end, str_sub, string = lines) |> 
        map(str_trim) |> 
        `names<-`(column_headers)
    ) |> 
    pull(columns) |> 
    bind_cols() |> 
    mutate(across(everything(), ~ if_else(.x == "", NA_character_, .x)))
  
  wrapped_text_rows <- table_initial$`#` |> 
    is.na() |> 
    which()
  
  if(length(wrapped_text_rows) == 0) {
    return(table_initial)
  }
  
  stopifnot(length(wrapped_text_rows) %% 2 == 0) # stop if not even
  
  alt_number_table <- {length(wrapped_text_rows)/2} |> 
    seq() |> 
    map(~wrapped_text_rows[c(.x * 2) +c(-1, 0)]) |> 
    map(~{
      non_missing_index <- .x[1] + 1
      
      tibble(
        row_index = .x,
        alt_number = table_initial$`#`[[non_missing_index]] 
      )
    }) |> 
    bind_rows()
  
  out <- table_initial |> 
    mutate(
      row_index = seq_along(`#`)
    ) |> 
    left_join(alt_number_table, by = "row_index") |> 
    mutate(
      `#` = coalesce(`#`, alt_number)
    ) |> 
    select(-c("row_index", "alt_number")) |> 
    group_by(`#`) |> 
    summarise(
      across(everything(), ~.x |> na.omit() |>  paste(collapse = " "))
    )
  
  out
}

get_pdf_tables_combined <- function(url_path) {
  pdf_pages <- url_path |> 
    pdf_text()
  
  pdf_combined <- pdf_pages |> 
    str_remove_all("Coach\\:(.|\n)*") |> # get rid of coach stuff
    paste(collapse = "\n<NEWPAGE>\n")
  # paste(collapse = "\n")
  
  team_names <- pdf_combined |> 
    str_extract_all("\\b[\\w ]+\n\\#") |> 
    pluck(1) |> 
    str_extract("[\\w ]+\\b")
  
  # check team name as a pattern only occurs once
  team_names |> 
    map_int(str_count, string = pdf_combined)
  
  pdf_lines <- pdf_combined |> 
    str_split("\n") |> 
    pluck(1) |> 
    str_subset("^\\s*$", negate = TRUE) |> 
    str_remove("^ ")
  
  tibble(
    team_name = team_names
  ) |> 
    mutate(
      header_line = map_int(team_name, str_which, string = pdf_lines) + 1,
      end_line = c(header_line[-1] - 2, length(pdf_lines)),
      headers = map(header_line, get_headers, pdf_lines = pdf_lines),
      table_lines = map2(header_line + 1, end_line, ~pdf_lines[.x:.y]),
      table_lines_split = map(table_lines, ~ {
        new_pages <- (.x == "<NEWPAGE>") |> 
          which()
        
        start <- c(1, new_pages + 1)
        end <- c(new_pages - 1, length(.x))
        pdf_lines <- .x
        tibble(start, end) |> 
          filter(start <= end) |> 
          mutate(
            page_splits = map2(start, end, ~pdf_lines[.x:.y])
          ) |> 
          pull(page_splits)
      }),
      pdf_table = map2(table_lines_split, headers, ~{
        .x |> 
          map(get_pdf_table, column_headers = .y) |> 
          bind_rows()
      })
    ) |> 
    select(
      team_name, pdf_table
    ) |> 
    unnest(pdf_table)
  
}


# read html tables ----
draft_combine_data <- draft_combine_url |> 
  read_html() |> 
  html_table(header = TRUE) |> 
  pluck(1) |> 
  mutate(
    national_combine = TRUE
  )

state_combine_data <- state_combine_url |> 
  read_html() |> 
  html_table(header = TRUE) |> 
  pluck(1) |> 
  mutate(
    national_combine = FALSE
  )

combine_data <- bind_rows(
  draft_combine_data,
  state_combine_data
  )

# pdf scraping ----

u18_squads_pdf_tables_combined <- get_pdf_tables_combined(u18_squads_url)
u16_squads_pdf_tables_combined <- get_pdf_tables_combined(u16_squads_url) |> 
  mutate(
    ACADEMY = coalesce(ACADEMY, `STATE ACADEMY`)
  ) |> 
  select(-c("STATE ACADEMY"))


player_details_all <- read_parquet("players/data/raw/player_details_all.parquet")
player_details <- player_details_all |> 
  select(
    year, season_name, playerId, playerDetails.givenName, playerDetails.surname, 
    playerDetails.dateOfBirth, gamesPlayed, playerDetails.photoURL, playerDetails.heightCm,
    team.teamName
  )

combine_data_ids <- combine_data |> 
  mutate(
    team_name = case_when(
      `STATE LEAGUE CLUB` == "Claremont" ~ "Claremont Tigers",
      TRUE ~ `STATE LEAGUE CLUB`
    ) |> 
      str_remove_all(fixed(" (VFL)")) |> 
      str_replace_all("\\s", " ") |> #something weird is happening with spaces on Nyko
      str_replace_all(fixed("/"), "|") |>  # change slash to regex character
      str_replace_all(fixed(" Academy"), "( Academy)?") |>
      str_replace_all(fixed(" Cats"), "( Cats)?")
    ,
    playerId = pmap_chr(list(NAME, SURNAME, team_name), \(first_name, surname, team_name) {
      filtered_data <- player_details |> 
        filter(
          playerDetails.surname == surname, str_detect(team.teamName, regex(team_name,  ignore_case = TRUE)), year == 2023
        ) |> 
        distinct(playerId, .keep_all = TRUE) |> 
        mutate(
          first_letter_match = str_sub(playerDetails.givenName, 1, 1) == str_sub(first_name, 1, 1),
          first_name_match = (playerDetails.givenName == first_name)
        ) |> 
        filter(first_letter_match)
      
      if(nrow(filtered_data) != 1) {
        filtered_data <- filtered_data |> 
          filter(first_name_match)
        
      }
      
      if(nrow(filtered_data) != 1) {
        print(filtered_data)
        stop(paste("Count not get player", first_name, surname, "from team", team_name, "to match to a single person."))
      } 
      
      filtered_data |>
        pull(playerId)
      
    })
  )

# write data ----
write_parquet(u16_squads_pdf_tables_combined, "players/data/raw/u16_squads_pdf_tables_combined.parquet")
write_parquet(u18_squads_pdf_tables_combined, "players/data/raw/u18_squads_pdf_tables_combined.parquet")
write_parquet(combine_data, "players/data/raw/combine_data.parquet")
write_parquet(combine_data_ids, "players/data/raw/combine_data_ids.parquet")
