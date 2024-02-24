library(httr2)
library(dplyr)

get_bolter_player_data <- function(bolter_session_id) {
  req <- request("https://bolter.team/api/player/stats") |>
    req_headers(
      `content-type` = "application/json",
      cookie = paste0("boltersession=", bolter_session_id)
    ) |>
    req_body_json(list(
      name = "",
      team = "",
      position = "",
      onlyMyPlayers = 0,
      sort = "price",
      direction = "desc",
      pageSize = 1000,
      currentPage = 1
    ))
  
  # Perform the request
  res <- req_perform(req)
  
  
  res |> 
    resp_body_json() |> 
    _$data |> 
    bind_rows()
  
  
  res |> 
    resp_body_json() |> 
    _$data |> 
    bind_rows()
}
