library(dplyr)
library(fitzRoy)
library(httr)
library(arrow)

# not sure which yet:
competition_data <- read_parquet("https://github.com/bit-in-that/data-automation/raw/main/metadata/data/processed/competition_metadata_afl.parquet")
competition_data <- read_parquet("metadata/data/processed/competition_metadata_afl.parquet")


afl_cookie <- get_afl_cookie()


headers <- c('x-media-mis-token' = afl_cookie)
# season_code <- "CD_S2023014"
playerNameLike <- ""
teamId <- "" # c("CD_T100","CD_T1000","CD_T40","CD_T120","CD_T60","CD_T80","CD_T10","CD_T70","CD_T30","CD_T110","CD_T20","CD_T50","CD_T1010","CD_T90","CD_T140","CD_T130","CD_T150","CD_T160")
ageRanges <- ""
weightRanges <- ""
heightRanges <- ""
playerPosition <- "" #c("KEY_FORWARD","MEDIUM_DEFENDER","MEDIUM_FORWARD","MIDFIELDER","KEY_DEFENDER","RUCK","MIDFIELDER_FORWARD")
statesOfOrigin <- "" #c("SA","WA","VIC","NSW","NT","QLD","INT","TAS","ACT")
kickingFoot <- "" # c("LEFT", "RIGHT")
comp_code <- "014"
season_code <- "CD_S2001014"

function(
    afl_cookie = get_afl_cookie(),
    season_year
    )

api_url <- paste0("https://api.afl.com.au/statspro/playersStats/seasons/", 
                  season_code, 
                  "?playerNameLike=", playerNameLike,
                  "&teamId=",
                  "&ageRanges=",
                  "&heightRanges=",
                  "&weightRanges=",
                  "&playerPosition=",
                  "&kickingFoot=",
                  "&statesOfOrigin="
                  )

res <- GET(url = api_url, add_headers(headers))

context <- content(res)

context$search

context$totalResults

context$players[[1]]$team$teamId

player <- sapply(context$players, \(x) paste0(x$playerDetails$givenName, " ", x$playerDetails$surname, " (", x$playerDetails$kickingFoot,")"))
positions <- sapply(context$players, \(x) x$playerDetails$position)
statesOfOrigin <- sapply(context$players, \(x) x$team$teamId)



player |> 
  clipr::write_clip()
