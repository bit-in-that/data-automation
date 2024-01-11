library(httr2)
library(purrr)
library(tibble)
library(arrow)

session_id <- "b16a3d90377b0ddd9ab0f7ab15a8dbc904082d6a"
# session_id2 <- "d7f149554717df7d3062aa49f379a51b06537833"

handle_data <- function(resp) {
  resp_json <- resp |> 
    resp_body_json()
  
  if(resp_json$success == 1) {
    with(resp_json$result, {
      tibble(
        team_name = name,
        team_id = id,
        user_id = user_id,
        firstname = firstname,
        lastname = lastname
      )
    })
    
  } else {
    NULL
    
  }
}


params <- list(round_num = NULL)
headers <- list(cookie = paste0("session=", session_id))

base_req <- request("https://fantasy.afl.com.au/afl_classic/api/teams_classic/show") |> 
  req_url_query(!!!params) |>
  req_headers(!!!headers)

req_list <- ((1:39200)) |> 
  map(~base_req |> req_url_query(id = .x))

system.time({
  resp_list <- req_list |> 
    req_perform_parallel(on_error = "return")
  
})

system.time({
  resp_tbl <- resp_list |> 
    resps_successes() |>
    resps_data(handle_data)
})

resp_tbl |> 
  write_parquet("afl_fantasy/data/raw/2024/afl_fantasy_team_ids.parquet")

