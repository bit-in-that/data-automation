library(dplyr)
library(httr)
library(stringr)
library(arrow)

response <- GET("https://aflapi.afl.com.au/afl/v2/competitions?pageSize=50") # doesn't provide SANFL if you don't increase the page size
output <- content(response)

competition_metadata_afl <- output$competitions |> 
  bind_rows() |> 
  mutate(
    comp_code = str_remove_all(providerId, "[^\\d]"),
    code = coalesce(code, ""),
    is_womens = str_detect(code,"[WG]$"),
    is_junior = str_detect(code,"[BG]$"),
    is_state_level  = !id %in% 1:3,
    name_lookup = if_else(str_detect(name, "^NAB League "), paste0(name, " (Legacy)"), name) |> 
      str_replace_all("(NAB League|Coates Talent League) ", "Victorian U18 ") |> 
      str_remove_all("(Toyota |NAB |Coates Talent League| Premiership)")
  )

competition_name_lookup_options <- c("AFL","AFL Preseason","AFLW","Victorian U18 Boys (Legacy)",
                                     "Victorian U18 Girls","Victorian U18 Girls (Legacy)","VFL","",
                                     "State of Origin","VFLW","WAFL","Victorian U18 Boys","SANFL")
  
if(!identical(competition_name_lookup_options, competition_metadata_afl$name_lookup)) {
  stop("Double check the name lookups before saving the data, there may be a new competition or a change of name.")
} else {
  write_parquet(competition_metadata_afl, "metadata/data/processed/competition_metadata_afl.parquet")
}

  


