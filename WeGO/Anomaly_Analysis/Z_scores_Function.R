library(tidyverse) 
library(dtplyr)
library(readr)
library(lubridate)
library(parallel)
library(foreach)
library(kableExtra)
library(parallelMap)
library(doParallel) # future
library(arrow) #Rspark
library(tidytransit)
library(sparklyr)


get_z_scores <- function(anom_df){
  
  require(tidyverse)
  
  trip_ids <- anom_df$trip_id
  vehicle_ids <- anom_df$vehicle_id
  
  z_scores <- anom_df[ , -(which(names(anom_df) %in% c('trip_id', 'service_id', 'shape_id', 'month', 'Hour', 'vehicle_id')))]
  
  heat_z_score <- z_scores %>%
    rownames_to_column() %>%
    gather(colname, value, -rowname)
  
  colnames(heat_z_score) <- c('trip_id', 'stop_id',  'ons')
  
  heat_z_score2 <- heat_z_score %>%
    mutate(ons = as.numeric(ons)) %>%
    filter(complete.cases(ons)) %>%
    group_by(stop_id) %>%
    mutate(z_score = (ons - mean(ons))/sd(ons))
  
  df_heat_z_score <- left_join(heat_z_score, heat_z_score2, by = c('trip_id', 'stop_id'))
  df_heat_z_score <- df_heat_z_score[ , -c(3)]
  colnames(df_heat_z_score) <- c('trip_id', 'stop_id', 'ons', 'z_score')
  
  df_heat_z_score$trip_id <- factor(trip_ids)
  df_heat_z_score$vehicle_id <- factor(vehicle_ids)
  
  return(df_heat_z_score)
  
}