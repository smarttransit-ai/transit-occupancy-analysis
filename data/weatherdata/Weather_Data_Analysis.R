###############################################################################
##### Weather data estimation using geostatistics
###############################################################################

# Required libraries:

library(arrow)          # It reads *.parquet files.
library(tidyverse)      # Data wrangling. This is similar to Pandas.
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
wego_df <- read_csv('Data/WeGO_APC_and_Weather.csv')


#-------------------------------------------------------------------------------
# Temperature time series:
#-------------------------------------------------------------------------------

weather_df %>%
  ggplot(aes(x = timestamp_local, y = temp)) +
  geom_line()


#-------------------------------------------------------------------------------
# Precipitation time series:
#-------------------------------------------------------------------------------

weather_df %>%
  ggplot(aes(x = timestamp_local, y = precip)) +
  geom_line()


#-------------------------------------------------------------------------------
# Weather stations:
#-------------------------------------------------------------------------------

weather_stations <- data.frame(
  'station_id' <- weather_df$station_id,
  'x' <- weather_df$gps_coordinate_longitude,
  'y' <- weather_df$gps_coordinate_latitude
)

weather_stations <- unique(weather_stations)
colnames(weather_stations) <- c('station_id', 'x', 'y')

coordinates(weather_stations) <- ~x+y
proj4string(weather_stations) <- CRS("+init=epsg:32136")

plot(weather_stations)

#-------------------------------------------------------------------------------
# WeGO stations:
#-------------------------------------------------------------------------------

wego_stations <- data.frame(
  'station_id' <- wego_df$stop_id,
  'x' <- wego_df$map_longitude,
  'y' <- wego_df$map_latitude
)

wego_stations <- unique(wego_stations)
colnames(wego_stations) <- c('stop_id', 'x', 'y')

coordinates(wego_stations) <- ~x+y
proj4string(wego_stations) <- CRS("+init=epsg:32136")

plot(wego_stations)

#shapefile(wego_stations, filename = 'wego_stations.shp')

################################################################################
# Temperature dataframe:

Weather_DF <- weather_df %>%
  mutate(local_hour = format(as.POSIXct(timestamp_local), format = '%H')) %>%
  select(station_id, gps_coordinate_latitude,gps_coordinate_longitude,
         timestamp_utc, timestamp_local, temp, precip, month, year,
         window, day, local_hour)

# Delete main dataframe:

#rm(weather_df)

#-------------------------------------------------------------------------------
# We select the relevant features for temperature to deal with a smaller
# dataset:

temperature_df <- weather_df %>%
  select(station_id, gps_coordinate_latitude,gps_coordinate_longitude,
         timestamp_utc, timestamp_local, temp, month, year, window, day, hour)

# Then, we convert the dataframe to a spatialdataframe:

colnames(temperature_df)[2:3] <- c('y', 'x')

coordinates(temperature_df) <- ~x+y
proj4string(temperature_df) <- CRS("+init=epsg:32136")

################################################################################
# Illustrative example using from 01-01-2017 at 6 pm:

temperature_df1_18 <- weather_df %>%
  mutate(local_hour = format(as.POSIXct(timestamp_local), format = '%H')) %>%
  filter(month == 1, year == 2017, day == 1, local_hour == '18') %>%
  select(station_id, gps_coordinate_latitude,gps_coordinate_longitude,
         timestamp_utc, timestamp_local, temp, month, year, window, day, hour)

# Converting to a spatialdataframe:

colnames(temperature_df1_18)[2:3] <- c('y', 'x')

temperature_df1_18_sp <- temperature_df1_18

coordinates(temperature_df1_18_sp) <- ~x+y
proj4string(temperature_df1_18_sp) <- CRS("+init=epsg:32136")

#-------------------------------------------------------------------------------
# Data histrogram:

hist(temperature_df1_18_sp@data$temp)

# The data seems to follow a Gaussian distribution, but we can double check it:

shapiro.test(temperature_df1_18_sp@data$temp)

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
                                    location = temperature_df1_18_sp,
                                    newdata = wego_stations, idp = 2)
summary(temp_vgm_18_fit_IDW_stations)

# The following plot shows that the data changes drastically in the
# N45W direction and is homogeneous in a perpendicular direction:

spplot(temp_vgm_18_fit_IDW_stations, 'var1.pred', colorkey = T)

################################################################################
## Variograms: The following function proceses the data for modeling.


temp_vgm_18 <- variogram(formula_temp,
                         data = temperature_df1_18_sp)


# Spherical model:

temp_vgm_18_fit_sph <- fit.variogram(temp_vgm_18,
                                     model = vgm(1, 'Sph', 0.75, 1))
plot(temp_vgm_18, temp_vgm_18_fit_sph)

# Gaussian model:

temp_vgm_18_fit_gau <- fit.variogram(temp_vgm_18, 
                                     model = vgm(1, 'Gau', 0.75, 1))
plot(temp_vgm_18, temp_vgm_18_fit_gau)

# Exponential model:

temp_vgm_18_fit_exp <- fit.variogram(temp_vgm_18,
                                     model = vgm(1, 'Exp', 0.75, 1))
plot(temp_vgm_18, temp_vgm_18_fit_exp)

# Stein model:

temp_vgm_18_fit_ste <- fit.variogram(temp_vgm_18,
                                     model = vgm(1, 'Ste', 0.75, 1))
plot(temp_vgm_18, temp_vgm_18_fit_ste)

# Stein model:

temp_vgm_18_fit_best_ste <- fit.variogram(temp_vgm_18,
                                          model = vgm(0, 'Ste', 0.77, 2))
plot(temp_vgm_18, temp_vgm_18_fit_best_ste)

#-------------------------------------------------------------------------------
### Goodness-of-fit:

attributes(temp_vgm_18_fit_sph)$SSErr
attributes(temp_vgm_18_fit_gau)$SSErr # This model is better compared with the rest.
attributes(temp_vgm_18_fit_exp)$SSErr
attributes(temp_vgm_18_fit_ste)$SSErr
attributes(temp_vgm_18_fit_best_ste)$SSErr

#-------------------------------------------------------------------------------
## Auto-Variogram:

temp_auto_vgm_18 <- autofitVariogram(formula_temp, temperature_df1_18_sp)
summary(temp_auto_vgm_18)

# I am not sure why the Stein model was selected over the Gaussian.

plot(temp_auto_vgm_18)

################################################################################
## Anisotropic Variogram:
################################################################################

ani_var_18 <- variogram(formula_temp, temperature_df1_18_sp,
                        map = TRUE, cutoff = 5,
                        width = 0.25)

plot(ani_var_18, col.regions = bpy.colors(64),
     main = 'Variogram Map',
     xlab = 'Longitude',
     ylab = 'Latitude')


plot(variogram(formula_temp, temperature_df1_18_sp,
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
                           temperature_df1_18_sp,
                           wego_stations, model = temp_vgm_18_fit_sph)

summary(temp_18_kriged_sph)

spplot(temp_18_kriged_sph, 'var1.pred', colorkey = T,
       xlab = 'Longitude', ylab = 'Latitude')

temp_18_kriged_gau = krige(formula_temp,
                           temperature_df1_18_sp,
                           wego_stations, model = temp_vgm_18_fit_gau)
summary(temp_18_kriged_gau)

spplot(temp_18_kriged_gau, 'var1.pred', colorkey = T,
       xlab = 'Longitude', ylab = 'Latitude')


temp_18_kriged_exp = krige(formula_temp,
                           temperature_df1_18_sp,
                           wego_stations, model = temp_vgm_18_fit_exp)
summary(temp_18_kriged_exp)

spplot(temp_18_kriged_exp, 'var1.pred', colorkey = T,
       xlab = 'Longitude', ylab = 'Latitude')

## Auto-kriging:

temp_18_auto_krige <- autoKrige(formula_temp,
                                temperature_df1_18_sp,
                                wego_stations)
summary(temp_18_auto_krige)

plot(temp_18_auto_krige)

#-------------------------------------------------------------------------------
# Function to extrapolate temperature:

temperature_estimate <- function(yr, mn, dy, hr, Output_format){
  
  options(warn=-1)
  
  #Wego_Stations = readOGR('wego_stations.shp')
  
  temp_df <- Weather_DF %>%
    filter(month == mn, year == yr, day == dy, local_hour == hr)
  #%>%
  #  select(station_id, gps_coordinate_latitude,gps_coordinate_longitude,
  #         timestamp_utc, timestamp_local, temp, month, year, window, day, hour)
    
  
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

temperature_estimate(2021, 2, 19, '12', "Combined")


#-------------------------------------------------------------------------------
# Function to extrapolate temperature:

precipitation_estimate <- function(yr, mn, dy, hr, Output_format){
  
  options(warn=-1)
  
  #Wego_Stations = readOGR('wego_stations.shp')
  
  precip_df <- Weather_DF %>%
    filter(month == mn, year == yr, day == dy, local_hour == hr)
  #%>%
  #  select(station_id, gps_coordinate_latitude,gps_coordinate_longitude,
  #         timestamp_utc, timestamp_local, temp, month, year, window, day, hour)
  
  
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

precipitation_estimate(2021, 2, 19, '12', "Combined")