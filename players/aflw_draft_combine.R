# packages ----
library(pdftools)
library(rvest)
library(purrr)
library(dplyr)
library(stringr)
library(tidyr)
library(arrow)

# paths ----
draft_combine_url <- "https://www.afl.com.au/aflw/news/1025092/2023-aflw-draft-combine-list-announced"
# not bothering with U16 because it was causing problems that are not worth fixing
# u16_squads_url <- "https://resources.afl.com.au/afl/document/2023/08/22/91d23a76-bfda-49c2-94ca-df915ed00404/2023-AFL-National-Development-Championships-U16-Girls-Squads.pdf"
u18_squads_url <- "https://resources.afl.com.au/afl/document/2023/08/22/064a4e80-1f3f-4ec9-9bc7-3c99b76f0797/2023-AFL-National-Championships-U18-Girls-Squads.pdf"
# u18_squads_url <- "../2023-AFL-National-Championships-U18-Girls-Squads.pdf"

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
  
  # manually fix
  line_to_fix <- pdf_lines |> 
    str_which("7\\s+Eva") |> 
    tail(n = 1)
  
  pdf_lines[line_to_fix] <- str_replace(pdf_lines[line_to_fix], "7", "8")
  
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
combine_data <- draft_combine_url |> 
  read_html() |> 
  html_table(header = TRUE) |> 
  pluck(1) |> 
  mutate(
    national_combine = TRUE
  )

# pdf scraping ----

u18_squads_pdf_tables_combined <- get_pdf_tables_combined(u18_squads_url)
u18_squads_pdf_tables_combined <- u18_squads_pdf_tables_combined |> 
  mutate(
    SURNAME = case_when(
      SURNAME == "Weston- Lee Turner" ~ "Weston-Turner",# manual fix
      str_detect(SURNAME, paste0("^", str_sub(NAME, -1, -1), "\\s+")) ~ 
        str_remove(SURNAME, paste0("^", str_sub(NAME, -1, -1), "\\s+")), # fix issues with ending last character of first name flowing over to the surname
      TRUE ~ SURNAME
    )
  )

# Gabrielle Trajer and Charlotte day are causing problems
# u16_squads_pdf_tables_combined <- get_pdf_tables_combined(u16_squads_url) #|> 
  # mutate(
  #   ACADEMY = coalesce(ACADEMY, `STATE ACADEMY`)
  # ) |> 
  # select(-c("STATE ACADEMY"))


player_details_all <- read_parquet("players/data/raw/player_details_all.parquet")
player_details <- player_details_all |> 
  select(
    year, season_name, playerId, playerDetails.givenName, playerDetails.surname, 
    playerDetails.dateOfBirth, gamesPlayed, playerDetails.photoURL, playerDetails.heightCm,
    team.teamName
  )

player_details_sanflw <- read_parquet("state_leagues/data/raw/player_details_sanfl.parquet") |>
  filter(competition == "womens")

player_details_waflw <- read_parquet("state_leagues/data/raw/player_details_wafl.parquet") |> 
  filter(gender == "Female")

combine_data_ids_wa <- combine_data |>  
  filter(STATE == "WA") |>
  mutate(
    team_name = case_when(
      TRUE ~ `STATE LEAGUE CLUB`
      ),
    playerId_wafl = pmap_chr(list(NAME, SURNAME, team_name), \(player_first_name, player_surname, team_name) {
      filtered_data <- player_details_waflw |> 
        rename(playerId = id) |> 
        filter(
          str_to_lower(last) == str_to_lower(player_surname), str_detect(club_name, regex(team_name,  ignore_case = TRUE))
        ) |> 
        distinct(playerId, .keep_all = TRUE) |> 
        mutate(
          first_letter_match = str_sub(first, 1, 1) == str_sub(first, 1, 1),
          first_name_match = (first == player_first_name)
        ) |> 
        filter(first_letter_match)
      
      if(nrow(filtered_data) != 1) {
        
        filtered_data <- filtered_data |> 
          filter(first_name_match)
        
      }
      
      if(nrow(filtered_data) != 1) {
        print(filtered_data)
        stop(paste("Count not get player", player_first_name, player_surname, "from team", team_name, "to match to a single person."))
      } 
      
      filtered_data |>
        pull(playerId)
      })
  )
  
combine_data_ids_sa <- combine_data |>  
  filter(STATE == "SA") |> 
  mutate(
    team_name = case_when(
      `STATE LEAGUE CLUB` == "Woodville-West Torrens" ~ "Eagles",
      TRUE ~ `STATE LEAGUE CLUB`
      ) |> 
      paste0("( \\(w\\))?"),
    playerId_sanfl = pmap_chr(list(NAME, SURNAME, team_name), \(player_first_name, player_surname, team_name) {
      filtered_data <- player_details_sanflw |> 
        filter(
          surname == player_surname, str_detect(team, regex(team_name,  ignore_case = TRUE)), season_year %in% 2021:2023
        ) |> 
        distinct(playerId, .keep_all = TRUE) |> 
        mutate(
          first_letter_match = str_sub(firstname, 1, 1) == str_sub(player_first_name, 1, 1),
          first_name_match = (firstname == player_first_name)
        ) |> 
        filter(first_letter_match)
      
      if(nrow(filtered_data) != 1) {
        
        filtered_data <- filtered_data |> 
          filter(first_name_match)
        
      }
      
      if(nrow(filtered_data) != 1) {
        print(filtered_data)
        stop(paste("Count not get player", player_first_name, player_surname, "from team", team_name, "to match to a single person."))
      } 
      
      filtered_data |>
        pull(playerId)
      })
  ) |> 
  mutate(
    playerId = paste0("CD_I", playerId_sanfl)
  )
  

combine_data_ids_vic <- combine_data |> 
  filter(
    ! STATE %in% c("SA", "WA"),
    !`STATE LEAGUE CLUB` %in% c("Swan Districts", "")
  ) |> 
  mutate(
    team_name = case_when(
      `STATE LEAGUE CLUB` == "Claremont" ~ "Claremont Tigers",
      `STATE LEAGUE CLUB` == "Northern Territory Academy" ~ "(Northern Territory U18 Womans Football Club|Northern Territory Academy)",
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
          playerDetails.surname == surname, str_detect(team.teamName, regex(team_name,  ignore_case = TRUE)), year %in% 2022:2023
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

aflw_combine_data_ids <- bind_rows(
  combine_data_ids_wa, combine_data_ids_sa, combine_data_ids_vic
) |> 
  select(-team_name)

# write data ----
# write_parquet(u16_squads_pdf_tables_combined, "players/data/raw/aflw_u16_squads_pdf_tables_combined.parquet")
write_parquet(u18_squads_pdf_tables_combined, "players/data/raw/aflw_u18_squads_pdf_tables_combined.parquet")
write_parquet(combine_data, "players/data/raw/aflw_combine_data.parquet")
write_parquet(aflw_combine_data_ids, "players/data/raw/aflw_combine_data_ids.parquet")
