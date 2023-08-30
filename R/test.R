cat(getwd())
cat(here::here())

x <- system("ipconfig", intern=TRUE)
z <- x[grep("IPv4", x)]
print(gsub(".*? ([[:digit:]])", "\\1", z))
