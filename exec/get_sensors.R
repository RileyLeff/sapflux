get_sensors <- function(path = "data/sensors.csv"){
    sens <- read.csv(path)
    return(sens)
}