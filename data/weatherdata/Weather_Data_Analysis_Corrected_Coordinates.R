################################################################################
## New weather data with corrected lat-lon coordinates
################################################################################

# Required libraries:

library(arrow)          # It reads *.parquet files.
library(tidyverse)      # Data wrangling. This is similar to Pandas.
library(dplyr)
library(rgdal)          # Reads and writes spatial files (e.g., *.shp).
library(sp)             # Spatial data operations.
library(spacetime)      # Spatio-temporal analysis.
library(sf)             # Spatial features.
library(readr)          # Reads *.csv files.
library(gstat)          # Performs variogram fitting.
library(raster)         # Spatial data plots.
library(lubridate)      # For temporal data formats.
library(automap)        # Automatized variogram kriging model selection.
library(parallel)
library(foreach)
library(parallelMap)
library(doParallel)
#-------------------------------------------------------------------------------
# Datasets:
#-------------------------------------------------------------------------------

# Weather data:

weather_df <- read_parquet('Data/tn_correct_lat_long_weather_data_no_temp_nulls.parquet')

# WeGO data:

wego_df <- read_parquet('Data/WeGO_APC_and_Old_Weather.parquet')

#-------------------------------------------------------------------------------
# Temperature time series:
#-------------------------------------------------------------------------------

weather_df %>%
  ggplot(aes(x = timestamp_local, y = temp)) +
  geom_line()


#-------------------------------------------------------------------------------
# Weather stations:
#-------------------------------------------------------------------------------

# All weather stations:

## Weather stations:

weather_stations <- weather_df %>%
  dplyr::select(station_id, lat, lon)

weather_stations <- unique(weather_stations)

# Training stations:

set.seed(123)
train_weather_stations <- sample((1:nrow(weather_stations)),
                                 floor(0.8*nrow(weather_stations)),
                                 replace = F)

weather_stations_train <- weather_stations[train_weather_stations, ]

coordinates(weather_stations_train) <- ~lon+lat
proj4string(weather_stations_train) <- CRS("+init=epsg:32136")

plot(weather_stations_train, pch = 10, col = 'blue')


# Test stations:

weather_stations_test <- weather_stations[-train_weather_stations, ]

coordinates(weather_stations_test) <- ~lon+lat
proj4string(weather_stations_test) <- CRS("+init=epsg:32136")


## Plot:

coordinates(weather_stations) <- ~lon+lat
proj4string(weather_stations) <- CRS("+init=epsg:32136")

plot(weather_stations)
plot(weather_stations_train, pch = 10, col = 'blue', add = T)
plot(weather_stations_test, pch = 10, col = 'red', add = T)


#-------------------------------------------------------------------------------
# WeGO stations:
#-------------------------------------------------------------------------------

wego_stations <- read_csv('Data/wego_stations.csv')

coordinates(wego_stations) <- ~x+y
proj4string(wego_stations) <- CRS("+init=epsg:32136")
plot(wego_stations, pch = 1, col = 'green', add = T)



################################################################################
# Temperature dataframe:

Weather_DF <- weather_df %>%
  dplyr::mutate(local_hour = format(as.POSIXct(timestamp_local), format = '%H')) %>%
  dplyr::select(station_id, lat,lon,
                timestamp_local, snow, temp, precip, month, year,
                day, local_hour)

# Delete main dataframe:

rm(weather_df)



################################################################################
#-------------------------------------------------------------------------------
# Function to extrapolate temperature:



temperature_estimate <- function(date, hr, Output_format){
  
  options(warn=-1)
  
  date_input = ymd(date)
  
  yr = year(date_input)
  mn= month(date_input)
  dy = day(date_input)
  
  temp_df <- Weather_DF %>%
    filter(month == mn, year == yr, day == dy, local_hour == hr)
  
  colnames(temp_df)[2:3] <- c('y', 'x')
  
  coordinates(temp_df) <- ~x+y
  proj4string(temp_df) <- CRS("+init=epsg:32136")
  
  formula_temp <- as.formula(temp ~ x + y)
  
  ## Auto-Variogram:
  
  temp_auto_vgm <- autofitVariogram(formula_temp,
                                    temp_df)
  
  temp_auto_krige <- autoKrige(formula_temp,
                               temp_df,
                               wego_stations)
  
  ## Auto-Kriging:
  
  temp_auto_krige <- autoKrige(formula_temp,
                               temp_df,
                               wego_stations)
  
  temp_estimates <- data.frame(temp_auto_krige[["krige_output"]]@data)
  colnames(temp_estimates) <- c('Predicted_temp', 'Variance', 'St_Dev')
  temp_estimates$date <- date_input
  temp_estimates$hour <- hr
  
  
  if(Output_format == 'Estimate'){
    
    return(temp_estimates)
    
  }
  else if(Output_format == 'Combined'){
    
    output = data.frame(wego_stations@data, temp_estimates)
    return(output)
    
  }
  else{
    
    return(temp_estimates$Predicted_temp) 
    
  }
  
  
}

temperature_estimate('2020-01-01', '03', "Combined")

# ("2020-01-09", "2020-01-12", "2020-01-13", "2020-01-14", "2020-01-15")
################################################################################
# Estimates:

#Dates_list <- ymd(unique(wego_df$transit_date))
Dates_list <- c("2021-01-11", "2021-01-12", "2021-01-13", "2021-01-14", "2021-01-15")
hours <- c(6:23)
hours <- if_else(hours < 10, paste(0, hours, sep = ''), as.character(hours))

# Temperature:

Nashville_temp_estimates <- data.frame()
Nashville_temp_estimates2 <- data.frame()
Nashville_temp_estimates3 <- data.frame()
# 64, 117

time0_surv = Sys.time()
cl <- makePSOCKcluster(8)
registerDoParallel(cl)
for(idate in Dates_list){
  for(ihour in hours){
    x_temp <- temperature_estimate(idate, ihour, "Combined")
    Nashville_temp_estimates <- rbind(Nashville_temp_estimates, x_temp)
  }
  
}
stopCluster(cl)
time1_surv = Sys.time()

#Computation_time1 = time1_surv - time0_surv
Computation_time2 = time1_surv - time0_surv

write_parquet(Nashville_temp_estimates, 'Data/Nashville_temp_estimates.parquet')



time0_surv = Sys.time()
cl <- makePSOCKcluster(8)
registerDoParallel(cl)
for(i in (118:length(Dates_list))){
  for(j in (1:length(hours))){
    x_temp <- temperature_estimate(Dates_list[i], hours[j], "Combined")
    Nashville_temp_estimates3 <- rbind(Nashville_temp_estimates2, x_temp)
  }
  
}
stopCluster(cl)
time1_surv = Sys.time()

#Computation_time1 = time1_surv - time0_surv
Computation_time2 = time1_surv - time0_surv


###########################################################################################



precipitation_estimate <- function(date, hr, Output_format){
  
  options(warn=-1)
  
  date_input = ymd(date)
  
  yr = year(date_input)
  mn= month(date_input)
  dy = day(date_input)
  
  precip_df <- Weather_DF %>%
    filter(month == mn, year == yr, day == dy, local_hour == hr)
  
  colnames(precip_df)[2:3] <- c('y', 'x')
  
  coordinates(precip_df) <- ~x+y
  proj4string(precip_df) <- CRS("+init=epsg:32136")
  
  formula_precip <- as.formula(precip ~ x + y)
  
  ## Auto-Variogram:
  
  precip_auto_vgm <- idw(formula = formula_precip,
                         location = precip_df,
                         newdata = wego_stations,
                         idp = 2)
  
  precip_estimates <- data.frame(precip_auto_vgm@data)
  colnames(precip_estimates) <- c('Predicted_precip', 'Variance')
  precip_estimates$date <- date_input
  precip_estimates$hour <- hr
  
  if(Output_format == 'Estimate'){
    
    return(precip_estimates)
    
  }
  else if(Output_format == 'Combined'){
    
    output = data.frame(wego_stations@data, precip_estimates)
    return(output)
    
  }
  else{
    
    return(precip_estimates$Predicted_precip) 
    
  }
  
}

precipitation_estimate('2020-01-01', '03', "Combined")


Nashville_precip_estimates <- data.frame()

time0_surv = Sys.time()
cl <- makePSOCKcluster(8)
registerDoParallel(cl)
for(idate in Dates_list){
  for(ihour in hours){
    x_precip <- precipitation_estimate(idate, ihour, "Combined")
    Nashville_precip_estimates <- rbind(Nashville_precip_estimates, x_precip)
  }
  
}
stopCluster(cl)
time1_surv = Sys.time()

Computation_time2 = time1_surv - time0_surv

write_parquet(Nashville_precip_estimates, 'Data/Nashville_precip_estimates.parquet')

#########################################################################################
#-------------------------------------------------------------------------------
# Function to extrapolate cummulatie snow (day before):

cum_snow_estimate <- function(date, hr, Output_format){
  
  options(warn=-1)
  
  date_input = ymd(date)
  time_input = hms(paste(hr, '00', '00', sep = ':'))
  
  date_time_input = ymd_hms(paste(date, paste(hr, '00', '00', sep = ':'), sep = ' '))
  date_time0_input = date_time_input - hms('24:00:00')
  
  yr1 = year(date_input)
  mn1= month(date_input)
  dy1 = day(date_input)
  
  yr0 = year(date_time0_input)
  mn0 = month(date_time0_input)
  dy0 = day(date_time0_input)
  
  snow_df <- Weather_DF %>%
    filter(timestamp_local >= date_time0_input, timestamp_local <= date_time_input) %>%
    group_by(station_id, local_hour) %>%
    mutate(cum_sum_snow = sum(snow), 
           x = gps_coordinate_longitude,
           y = gps_coordinate_latitude)
  
  colnames(snow_df)[2:3] <- c('y', 'x')
  
  coordinates(snow_df) <- ~x+y
  proj4string(snow_df) <- CRS("+init=epsg:32136")
  
  formula_cum_snow <- as.formula(cum_sum_snow ~ x + y)
  
  ## Auto-Variogram:
  
  snow_idw <-  idw(formula = formula_cum_snow,
                   location = snow_df,
                   newdata = wego_stations,
                   idp = 2)
  
  ## Auto-Kriging:
  
  cum_snow_estimates <- data.frame(snow_idw@data)
  colnames(cum_snow_estimates) <- c('Predicted_cum_snow', 'Variance')
  cum_snow_estimates$date <- date_input
  cum_snow_estimates$hour <- hr
  
  
  if(Output_format == 'Estimate'){
    
    return(cum_snow_estimates)
    
  }
  else if(Output_format == 'Combined'){
    
    output = data.frame(wego_stations@data, cum_snow_estimates)
    return(output)
    
  }
  else{
    
    return(cum_snow_estimates$Predicted_cum_snow) 
    
  }
  
}

cum_snow_estimate('2020-01-01', '03', "Combined")


Nashville_cum_snow_estimates <- data.frame()

time0_surv = Sys.time()
cl <- makePSOCKcluster(8)
registerDoParallel(cl)
for(idate in Dates_list){
  for(ihour in hours){
    x_cum_snow <- cum_snow_estimate(idate, ihour, "Combined")
    Nashville_cum_snow_estimates <- rbind(Nashville_cum_snow_estimates, x_cum_snow)
  }
  
}
stopCluster(cl)
time1_surv = Sys.time()

Computation_time2 = time1_surv - time0_surv

write_parquet(Nashville_cum_snow_estimates, 'Data/Nashville_cum_snow_estimates.parquet')
