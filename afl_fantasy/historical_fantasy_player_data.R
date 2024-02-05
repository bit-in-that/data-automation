library(httr2)
library(dplyr)


base_req <- request("https://fantasy.afl.com.au/data/afl/archive")

x <- base_req |> 
  req_url_path_append("2014/players.json") |> 
  req_perform() |> 
  resp_body_json()

y <- base_req |> 
  req_url_path_append("2023/players.json") |> 
  req_perform() |> 
  resp_body_json()

"https://fantasy.afl.com.au/data/afl/archive/2014/players.json"


x[[1]]$stats$selections_info

names(x[[1]])
names(y[[1]])
