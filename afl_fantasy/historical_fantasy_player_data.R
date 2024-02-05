library(httr2)
library(dplyr)
library(purrr)
library(jsonlite)

handle_players_json_data <- function(resp) {
  if(resp$headers$`Content-Type` == "application/json") {
    parsed_data <- resp |> 
      resp_body_json()
  } else {
    parsed_data <- resp |> 
      resp_body_string() |> 
      parse_json()
  }
  out <- parsed_data |> 
    map(~{
      .x |> 
        map_if(is.list, list)
    }) |> 
    bind_rows() |> 
    mutate(
      id = as.character(id),
      squad_id = as.character(squad_id),
      cost = as.integer(cost)
    )
  
  if("stats_u18" %in% colnames(out)) {
    out |> 
      select(-stats_u18) #always missing
  } else {
    out
  }
  
}

headers <- list("Content-Type" = "application/json")
base_req <- request("https://fantasy.afl.com.au/data/afl/archive") |> 
  req_headers(!!!headers)


req_list <- 2014:2023 |>
  map(~base_req |> req_url_path_append(.x, "players.json"))

system.time({
  resp_list <- req_list |> 
    # req_perform_parallel(on_error = "return")
    req_perform_sequential()
  
})

system.time({
  resp_tbl <- resp_list |> 
    resps_successes() |>
    resps_data(handle_players_json_data)
})

z <- resp_list[[1]] |> 
  resp_body_json()

z[[1]]$stats_u18

# ==============================================================================

x <- base_req |> 
  req_url_path_append("2014/players.json") |> 
  req_perform() |> 
  resp_body_json()

y <- base_req |> 
  req_url_path_append("2023/players.json") |> 
  req_perform() |> 
  resp_body_json()


yyy <- y |> 
  map(~{
    .x |> 
      map_if(is.list, list)
  }) |> 
  bind_rows()

xxx |> 
  bind_rows(yyy) |> 
  View()



x[[1]]$stats$selections_info

(xx <- names(x[[1]]$stats))
(yy <- names(y[[1]]$stats))

setdiff(xx, yy)
setdiff(yy, xx)

y[[1]]$stats$career_avg_vs
