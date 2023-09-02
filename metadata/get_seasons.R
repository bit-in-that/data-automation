library(dplyr)
library(httr)
library(stringr)
library(arrow)


competition_data <- read_parquet("metadata/data/processed/competition_metadata_afl.parquet")


