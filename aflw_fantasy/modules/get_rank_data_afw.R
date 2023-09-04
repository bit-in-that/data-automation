library(httr)
library(dplyr)
library(arrow)

source("aflw_fantasy/modules/get_squad_data_afw.R")

# will need the squad data later
get_rank_data <- function(
    session_id,
    squad_data = get_squad_data(),
    roundId = NULL, 
    page = 1,
    limit = 100000, #seems to be allowed to be arbitrarily large (bit of an oversite by the developers but good for us)
    direction = "ASC",
    state = NULL, # appears to be three letter state
    squadId = NULL # code of the club supported (comes from squads.json)
    ) {
  
  headers = c(
    cookie = paste0("X-SID=", session_id,";")
  )
  query <- list(
    roundId = roundId, 
    page = page,
    limit = limit, #seems to be allowed to be arbitrarily large (bit of an oversite by the developers but good for us)
    direction = direction,
    state = state, # appears to be three letter state
    squadId = squadId # code of the club supported (comes from squads.json)
  )
  
  response <- GET(
    url = "https://aflwfantasy.afl/api/en/fantasy/ranks/open-league",
    query = query, 
    add_headers(headers)
  )
  
  output <- content(response)
  
  rankings_data <- output$success$rankings |> 
    bind_rows() |> 
    left_join(squad_data, by = c("supportedSquadId" = "id")) |> 
    rename(supportedSquadName = name, supportedSquadAbbreviation = abbreviation) 
  
}

