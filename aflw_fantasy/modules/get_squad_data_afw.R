library(httr)
library(dplyr)

get_squad_data <- function() {
  response <- GET("https://aflwfantasy.afl/json/fantasy/squads.json")
  
  output <- content(response)
  
  output |> 
    bind_rows()
  
}
  