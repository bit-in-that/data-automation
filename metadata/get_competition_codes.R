library(dplyr)
library(httr)
library(stringr)
library(arrow)

response <- GET("https://aflapi.afl.com.au/afl/v2/competitions?pageSize=50") # doesn't provide SANFL if you don't increase the page size
output <- content(response)

competition_data <- output$competitions |> 
  bind_rows() |> 
  mutate(
    comp_code = str_remove_all(providerId, "[^\\d]"),
    code = coalesce(code, ""),
    is_womens = str_detect(code,"[WG]$"),
    is_junior = str_detect(code,"[BG]$"),
    is_state_level  = !id %in% 1:3,
    name_lookup = if_else(str_detect(name, "^NAB League "), paste0(name, " (Old)"), name) |> 
      str_replace_all("(NAB League|Coates Talent League) ", "Victorian U18 ") |> 
      str_remove_all("(Toyota |NAB |Coates Talent League| Premiership)")
  )

write_parquet(competition_data, "metadata/output/processed/competition_data.parquet")

