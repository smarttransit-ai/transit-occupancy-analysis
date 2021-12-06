###############################################################################
##### Weather data estimation using geostatistics
###############################################################################

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
#library(mapview)       # Interactive spatial data plots.
library(lubridate)      # For temporal data formats.
library(automap)        # Automatized variogram kriging model selection.

#-------------------------------------------------------------------------------
# Datasets:
#-------------------------------------------------------------------------------

# Weather data:

weather_df <- read_parquet('Data/all_weather_no_temp_null_2017_01_2021_10.parquet')

# WeGO data:

wego_df <- read_parquet('Data/WeGO_APC_and_Old_Weather.parquet')


#-------------------------------------------------------------------------------
# Temperature time series:
#-------------------------------------------------------------------------------

weather_df %>%
  ggplot(aes(x = timestamp_local, y = temp)) +
  geom_line()


#-------------------------------------------------------------------------------
# 24 hour cumulative snow time series:
#-------------------------------------------------------------------------------

weather_df %>%
  mutate(date = mdy(format(as.POSIXct(timestamp_local), format = '%m-%d-%Y'))) %>%
  group_by(date) %>%
  mutate(cum_sum_snow = sum(snow)) %>%
  ggplot(aes(x = date, y = cum_sum_snow)) +
  geom_line() + ylab('Cumulative Snow') +
  ggtitle('24 hour Cumulative Snow')


#-------------------------------------------------------------------------------
# Precipitation time series:
#-------------------------------------------------------------------------------

weather_df %>%
  ggplot(aes(x = timestamp_local, y = precip)) +
  geom_line()

#-------------------------------------------------------------------------------
# Weather stations:
#-------------------------------------------------------------------------------

# All weather stations:

## Weather stations:

weather_stations <- read_parquet('Data/weather_stations.parquet')

# Training stations:

set.seed(123)
train_weather_stations <- sample((1:nrow(weather_stations)),
                                 floor(0.8*nrow(weather_stations)),
                                 replace = F)

weather_stations_train <- weather_stations[train_weather_stations, ]

coordinates(weather_stations_train) <- ~x+y
proj4string(weather_stations_train) <- CRS("+init=epsg:32136")

plot(weather_stations_train, pch = 10, col = 'blue')


# Test stations:

weather_stations_test <- weather_stations[-train_weather_stations, ]

coordinates(weather_stations_test) <- ~x+y
proj4string(weather_stations_test) <- CRS("+init=epsg:32136")


## Plot:

coordinates(weather_stations) <- ~x+y
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
  dplyr::select(station_id, gps_coordinate_latitude,gps_coordinate_longitude,
         timestamp_local, snow, temp, precip, month, year,
         day, local_hour)

# Delete main dataframe:

rm(weather_df)

#-------------------------------------------------------------------------------
# We select the relevant features for temperature to deal with a smaller
# dataset:

temperature_df <- Weather_DF %>%
  dplyr::select(station_id, gps_coordinate_latitude,gps_coordinate_longitude,
                timestamp_local, temp, month, year, day, local_hour)

temperature_df_train <- temperature_df %>%
  dplyr::filter(station_id %in% weather_stations_train$station_id)

temperature_df_test <- temperature_df %>%
  dplyr::filter(station_id %in% weather_stations_test$station_id)

# Then, we convert the dataframe to a spatialdataframe:

temperature_df_sp_train <- temperature_df_train

#colnames(temperature_df_sp_train)[2:3] <- c('y', 'x')

#coordinates(temperature_df_sp_train) <- ~x+y
#proj4string(temperature_df_sp_train) <- CRS("+init=epsg:32136")

################################################################################
# Illustrative example using from 01-01-2017 at 6 pm:

#temperature_df1_18 <- temperature_df_train %>%
#  dplyr::filter(month == 1, year == 2020, day == 1, local_hour == '18') %>%
#  dplyr::select(station_id, gps_coordinate_latitude,gps_coordinate_longitude,
#                timestamp_local, temp, month, year, day, local_hour)

# Converting to a spatialdataframe:

colnames(temperature_df1_18)[2:3] <- c('y', 'x')

coordinates(temperature_df1_18) <- ~x+y
proj4string(temperature_df1_18) <- CRS("+init=epsg:32136")

#-------------------------------------------------------------------------------
# Data histrogram:

#hist(temperature_df1_18@data$temp)

# The data seems to follow a Gaussian distribution, but we can double check it:

#shapiro.test(temperature_df1_18@data$temp)

# Indeed, it seems that the observations follow a Gaussian distribution.

################################################################################
#################### Spatial Data Modeling #####################################
################################################################################


# We model temperature as a function of its spatial coordinates:

formula_temp <- as.formula(temp ~ x + y)

#-------------------------------------------------------------------------------
# IDW: This model highlights main trends, but it is not a spatial model.
#-------------------------------------------------------------------------------

temp_vgm_18_fit_IDW_stations <- idw(formula = formula_temp,
                                    location = temperature_df1_18,
                                    newdata = weather_stations_test, idp = 2)
summary(temp_vgm_18_fit_IDW_stations)

# The following plot shows that the data changes drastically in the
# N45W direction and is homogeneous in a perpendicular direction:

spplot(temp_vgm_18_fit_IDW_stations, 'var1.pred', colorkey = T)

################################################################################
## Variograms: The following function proceses the data for modeling.


temp_vgm_18 <- variogram(formula_temp,
                         data = temperature_df1_18)

plot(temp_vgm_18)
# Spherical model:

temp_vgm_18_fit_sph <- fit.variogram(temp_vgm_18,
                                     model = vgm(1, 'Sph', 0.75, 1))
plot(temp_vgm_18, temp_vgm_18_fit_sph)

# Gaussian model:

temp_vgm_18_fit_gau <- fit.variogram(temp_vgm_18, 
                                     model = vgm(1, 'Gau', 0.75, 1))

#summary(temp_vgm_18_fit_gau)
#plot(temp_vgm_18, temp_vgm_18_fit_gau)

# Exponential model:

temp_vgm_18_fit_exp <- fit.variogram(temp_vgm_18,
                                     model = vgm(1, 'Exp', 0.75, 1))
#plot(temp_vgm_18, temp_vgm_18_fit_exp)

# Stein model:

temp_vgm_18_fit_ste <- fit.variogram(temp_vgm_18,
                                     model = vgm(1, 'Ste', 0.75, 1))
#plot(temp_vgm_18, temp_vgm_18_fit_ste)

#-------------------------------------------------------------------------------
### Goodness-of-fit:

attributes(temp_vgm_18_fit_sph)$SSErr
attributes(temp_vgm_18_fit_gau)$SSErr
attributes(temp_vgm_18_fit_exp)$SSErr
attributes(temp_vgm_18_fit_ste)$SSErr

#-------------------------------------------------------------------------------
### Test RMSE:

true_temp <- temperature_df_test %>%
  dplyr::filter(month == 1, year == 2020, day == 1, local_hour == '18') 


temp_18_kriged_sph_test = krige(formula_temp,
                                temperature_df1_18,
                                weather_stations_test,
                                model = temp_vgm_18_fit_sph)

temp_18_kriged_sph_test_temps <- temp_18_kriged_sph_test@data[["var1.pred"]]


temp_18_kriged_gau_test = krige(formula_temp,
                                temperature_df1_18,
                                weather_stations_test,
                                model = temp_vgm_18_fit_gau)

temp_18_kriged_gau_test_temps <- temp_18_kriged_gau_test@data[["var1.pred"]]


temp_18_kriged_exp_test = krige(formula_temp,
                                temperature_df1_18,
                                weather_stations_test,
                                model = temp_vgm_18_fit_exp)

temp_18_kriged_exp_test_temps <- temp_18_kriged_exp_test@data[["var1.pred"]]


temp_18_kriged_ste_test = krige(formula_temp,
                                temperature_df1_18,
                                weather_stations_test,
                                model = temp_vgm_18_fit_ste)

temp_18_kriged_ste_test_temps <- temp_18_kriged_ste_test@data[["var1.pred"]]

temp_18_kriged_sph_test_temps_RMSE = sqrt(mean((temp_18_kriged_sph_test_temps - true_temp$temp)^{2}))
#temp_18_kriged_gau_test_temps_RMSE = sqrt(mean((temp_18_kriged_gau_test_temps - true_temp$temp)^{2}))
temp_18_kriged_exp_test_temps_RMSE = sqrt(mean((temp_18_kriged_exp_test_temps - true_temp$temp)^{2}))
temp_18_kriged_ste_test_temps_RMSE = sqrt(mean((temp_18_kriged_ste_test_temps - true_temp$temp)^{2}))

#-------------------------------------------------------------------------------
## Auto-Variogram:

temp_auto_vgm_18 <- autofitVariogram(formula_temp, temperature_df1_18)
summary(temp_auto_vgm_18)


# I am not sure why the Stein model was selected over the Gaussian.

plot(temp_auto_vgm_18)

temp_18_auto_krige_test <- autoKrige(formula_temp,
                                     temperature_df1_18,
                                     weather_stations_test)

temp_18_auto_krige_test_RMSE = sqrt(mean((temp_18_auto_krige_test[["krige_output"]]@data$var1.pred - true_temp$temp)^{2}))

################################################################################
## Anisotropic Variogram:
################################################################################

ani_var_18 <- variogram(formula_temp, temperature_df1_18,
                        map = TRUE, cutoff = 5,
                        width = 0.25)

plot(ani_var_18, col.regions = bpy.colors(64),
     main = 'Variogram Map',
     xlab = 'Longitude',
     ylab = 'Latitude')


plot(variogram(formula_temp, temperature_df1_18,
               alpha = c(30, 45, 90, 120),
               cutoff = 5),
     main = "Directional Variograms for temperature",
     sub = "Figure titles indicate azimuthal degrees")

################################################################################
#-------------------------------------------------------------------------------
# Kriging
#-------------------------------------------------------------------------------
################################################################################

temp_18_kriged_sph = krige(formula_temp, 
                           temperature_df1_18,
                           wego_stations,
                           model = temp_vgm_18_fit_sph)

summary(temp_18_kriged_sph)

spplot(temp_18_kriged_sph, 'var1.pred', colorkey = T,
       xlab = 'Longitude', ylab = 'Latitude')

temp_18_kriged_gau = krige(formula_temp,
                           temperature_df1_18,
                           wego_stations,
                           model = temp_vgm_18_fit_gau)
summary(temp_18_kriged_gau)

spplot(temp_18_kriged_gau, 'var1.pred', colorkey = T,
       xlab = 'Longitude', ylab = 'Latitude')


temp_18_kriged_exp = krige(formula_temp,
                           temperature_df1_18,
                           wego_stations, 
                           model = temp_vgm_18_fit_exp)
summary(temp_18_kriged_exp)

spplot(temp_18_kriged_exp, 'var1.pred', colorkey = T,
       xlab = 'Longitude', ylab = 'Latitude')

## Auto-kriging:

temp_18_auto_krige <- autoKrige(formula_temp,
                                temperature_df1_18,
                                wego_stations)
summary(temp_18_auto_krige)

plot(temp_18_auto_krige)

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
  
  temp_auto_vgm <- autofitVariogram(formula_temp, temp_df)
  
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


#-------------------------------------------------------------------------------
# Function to extrapolate precipitation:

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
  
  precip_auto_vgm <- autofitVariogram(formula_precip, precip_df)
  
  precip_auto_krige <- autoKrige(formula_precip,
                                 precip_df,
                                 wego_stations)
  
  ## Auto-Kriging:
  
  precip_auto_krige <- autoKrige(formula_precip,
                                 precip_df,
                                 wego_stations)
  
  precip_estimates <- data.frame(precip_auto_krige[["krige_output"]]@data)
  colnames(precip_estimates) <- c('Predicted_precip', 'Variance', 'St_Dev')
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

precipitation_estimate('2021-02-19', '12', "Combined")



#-------------------------------------------------------------------------------
# Function to extrapolate snow:

snow_estimate <- function(yr, mn, dy, hr, Output_format){
  
  options(warn=-1)
  
  
  snow_df <- Weather_DF %>%
    filter(month == mn, year == yr, day == dy, local_hour == hr)
  
  
  colnames(snow_df)[2:3] <- c('y', 'x')
  
  coordinates(snow_df) <- ~x+y
  proj4string(snow_df) <- CRS("+init=epsg:32136")
  
  formula_precip <- as.formula(snow ~ x + y)
  
  ## Auto-Variogram:
  
  snow_auto_vgm <- autofitVariogram(formula_precip, snow_df)
  
  snow_auto_krige <- autoKrige(formula_precip,
                               snow_df,
                               wego_stations)
  
  ## Auto-Kriging:
  
  snow_auto_krige <- autoKrige(formula_precip,
                               snow_df,
                               wego_stations)
  
  snow_estimates <- data.frame(snow_auto_krige[["krige_output"]]@data)
  colnames(snow_estimates) <- c('Predicted_snow', 'Variance', 'St_Dev')
  
  if(Output_format == 'Estimate'){
    
    return(snow_estimates)
    
  }
  else if(Output_format == 'Combined'){
    
    output = data.frame(wego_stations@data, snow_estimates)
    return(output)
    
  }
  else{
    
    return(snow_estimates$Predicted_snow) 
    
  }
  
}

snow_estimate(2021, 2, 19, '12', "Combined")


#-------------------------------------------------------------------------------
# Function to extrapolate cummulatie snow (day before):

cum_snow_estimate <- function(yr, mn, dy, hr, Output_format){ # Work it!
  
  options(warn=-1)
  
  
  snow_df <- Weather_DF %>%
    filter(month == mn, year == yr, day == dy, local_hour == hr) %>%
    mutate(date = mdy(format(as.POSIXct(timestamp_local), format = '%m-%d-%Y'))) %>%
    group_by(date) %>%
    mutate(cum_sum_snow = sum(snow))
  
  
  colnames(snow_df)[2:3] <- c('y', 'x')
  
  coordinates(snow_df) <- ~x+y
  proj4string(snow_df) <- CRS("+init=epsg:32136")
  
  formula_precip <- as.formula(cum_sum_snow ~ x + y)
  
  ## Auto-Variogram:
  
  snow_auto_vgm <- autofitVariogram(formula_precip, snow_df)
  
  snow_auto_krige <- autoKrige(formula_precip,
                               snow_df,
                               wego_stations)
  
  ## Auto-Kriging:
  
  snow_auto_krige <- autoKrige(formula_precip,
                               snow_df,
                               wego_stations)
  
  snow_estimates <- data.frame(snow_auto_krige[["krige_output"]]@data)
  colnames(snow_estimates) <- c('Predicted_snow', 'Variance', 'St_Dev')
  
  if(Output_format == 'Estimate'){
    
    return(snow_estimates)
    
  }
  else if(Output_format == 'Combined'){
    
    output = data.frame(wego_stations@data, snow_estimates)
    return(output)
    
  }
  else{
    
    return(snow_estimates$Predicted_snow) 
    
  }
  
}

snow_estimate(2021, 2, 19, '12', "Combined")

################################################################################
# Estimates:

Dates_list <- ymd(unique(wego_df$transit_date))
hours <- c(0:23)
hours <- if_else(hours < 10, paste(0, hours, sep = ''), as.character(hours))

# Temperature:

Nashville_temp_estimates <- data.frame()

for(i in (1:length(Dates_list))){
  for(j in (1:length(hours))){
    x_temp <- temperature_estimate(Dates_list[i], hours[j], "Combined")
    Nashville_temp_estimates <- rbind(Nashville_temp_estimates, x_temp)
  }
  
}

write_parquet(Nashville_temp_estimates, 'Nashville_temp_estimates.parquet')

Nashville_temp_estimates %>%
  mutate(date = ymd(date), stop_id = factor(stop_id), hour = as.numeric(hour)) %>%
  group_by(stop_id, date, hour) %>%
  ggplot(aes(x = hour, y = Predicted_temp, group = stop_id)) +
  geom_line(aes(colour = stop_id)) +
  facet_wrap(~date) +
  theme(legend.position = 'none')


# Precipitation:

Nashville_precip_estimates <- data.frame()

for(i in (1:length(Dates_list[2:10]))){
  for(j in (1:length(hours))){
    x_temp <- precipitation_estimate(Dates_list[i], hours[j], "Combined")
    Nashville_precip_estimates <- rbind(Nashville_precip_estimates, x_temp)
  }
  
}


Nashville_precip_estimates %>%
  mutate(stop_id = factor(stop_id), hour = as.numeric(hour),
         month = month(date)) %>%
  group_by(stop_id, date, hour) %>%
  ggplot(aes(x = hour, y = Predicted_precip, group = stop_id)) +
  geom_line(aes(colour = stop_id)) +
  facet_wrap(~date) +
  theme(legend.position = 'none')

precip_x <- Weather_DF %>%
  filter(year == 2021, month == 7, day == 5)

precip_x %>%
  mutate(local_hour = factor(local_hour)) %>%
  ggplot(aes(x = gps_coordinate_longitude, y = gps_coordinate_latitude, group = station_id)) +
  geom_point(aes(colour = precip)) +
  facet_wrap(~local_hour)
  


Nashville_precip_estimates2 <- precipitation_estimate('2021-07-05', '23', "Combined")
Nashville_precip_estimates2 <- data.frame(Nashville_precip_estimates2, wego_stations@coords)

head(Nashville_precip_estimates2)


radar_precip_sample_2021_10_21 <- st_read('Data/kohx_bref_raw.png.kml')
plot(radar_precip_sample_2021_10_21)


Wego_Stations_sf <- st_read(dsn = 'wego_stations.shp')

proj4string(Wego_Stations_sf) <- CRS("+init=epsg:32136") 
mapview(Wego_Stations_sf)
