library(tidyverse) 
#library(dtplyr)
library(readr)
library(lubridate)
library(parallel)
library(foreach)
library(kableExtra)
library(parallelMap)
library(doParallel) # future
library(arrow) #Rspark
library(tidytransit)
#library(sparklyr)


#-------------------------------------------------------------------------------

get_anomaly_matrix <- function(yr, mn, rt, di, service_type, pttn, hr){
  
  #dates_for_analysis <- as.Date(paste(yr, paste(0, mn, sep = ''), seq(1, days_in_month(mn)), sep = '-'))
  
  
  #date_name <- c('2_July_2021', '4_October_2021', '6_April_2021', '7_April_2021',
  #               '8_June_2021', '9_September_2020', '10_September_2020', '13_June_2020',
  #               '13_November_2019', '15_June_2021', '17_November_2020', '21_October_2020',
  #               '22_July_2021', '22_May_2020', '24_January_2020', '24_September_2021',
  #               '29_March_2020', '29_May_2020', '29_October_2020', '30_April_2021',
  #               '30_March_2020', '31_July_2020')
  
  #gtfs_dates <- as.Date(c('2021-07-02', '2021-10-04', '2021-04-06', '2021-04-07',
  #                        '2021-06-08', '2020-09-09', '2020-09-10', '2020-06-13',
  #                        '2019-11-13', '2021-06-15', '2020-11-17', '2020-10-21',
  #                        '2021-07-22', '2020-05-22', '2020-01-24', '2021-09-24',
  #                        '2020-03-29', '2020-05-29', '2020-10-29', '2021-04-30', 
  #                        '2020-03-30', '2020-07-31'))
  
  #GTFS_calendar_dates <- data.frame(
  #  date_name,
  #  gtfs_dates
  #)
  
  #GTFS_calendar_dates <- GTFS_calendar_dates[order(GTFS_calendar_dates$gtfs_dates),]
  
  #gtfs1_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[1]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[2]) - 1)
  
  #gtfs2_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[2]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[3])- 1)
  
  #gtfs3_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[3]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[4]) - 1)
  
  #gtfs4_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[4]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[5]) - 1)
  
  #gtfs5_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[5]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[6]) - 1)
  
  #gtfs6_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[6]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[7]) - 1)
  
  #gtfs7_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[7]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[8]) - 1)
  
  #gtfs8_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[8]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[9]) - 1)
  
  #gtfs9_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[9]), 
  #                           ymd(GTFS_calendar_dates$gtfs_dates[10]) - 1)
  
  #gtfs10_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[10]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[11]) - 1)
  
  #gtfs11_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[11]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[12]) - 1)
  
  #gtfs12_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[12]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[13]) - 1)
  
  #gtfs13_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[13]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[14]) - 1)
  
  #gtfs14_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[14]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[15]) - 1)
  
  #gtfs15_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[15]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[16]) - 1)
  
  #gtfs16_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[16]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[17]) - 1)
  
  #gtfs17_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[17]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[18]) - 1)
  
  #gtfs18_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[18]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[19]) - 1)
  
  #gtfs19_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[19]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[20]) - 1)
  
  #gtfs20_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[20]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[21]) - 1)
  
  #gtfs21_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[21]), 
  #                            ymd(GTFS_calendar_dates$gtfs_dates[22]) - 1)
  
  #gtfs22_interval <- interval(ymd(GTFS_calendar_dates$gtfs_dates[22]), 
  #                            ymd('2021-11-14'))
  
  #gtfs_intervals <- c(gtfs1_interval, gtfs2_interval, gtfs3_interval,
  #                    gtfs4_interval, gtfs5_interval, gtfs6_interval,
  #                    gtfs7_interval, gtfs8_interval, gtfs9_interval,
  #                    gtfs10_interval, gtfs11_interval, gtfs12_interval,
  #                    gtfs13_interval, gtfs14_interval, gtfs15_interval,
  #                    gtfs16_interval, gtfs17_interval, gtfs18_interval,
  #                    gtfs19_interval, gtfs20_interval, gtfs21_interval,
  #                    gtfs22_interval)
  
  #GTFS_calendar_dates$gtfs_intervals <- gtfs_intervals
  
  '%not_in%' <- Negate('%in%')
  
  #Get_GTFS_by_date <- function(input_date, gtfs_var){
    
  #  provided_date <- ymd(input_date)
    
  #  calendar_date <- GTFS_calendar_dates$date_name[sapply(GTFS_calendar_dates$gtfs_intervals, function(x) { mapply(`%within%`, provided_date, x)})]
    
  #  gtfs_path <- paste('MTA_GTFS', calendar_date, 'gtfs.zip', sep = '/')
    
  #  required_gtfs <- tidytransit::read_gtfs(gtfs_path)
    
  #  if(gtfs_var == 'calendar'){
  #    out_data <- required_gtfs[['calendar']]
  #  }
  #  else if(gtfs_var == 'calendar_dates'){
  #    out_data <- required_gtfs[['calendar_dates']]
  #  }
  #  else if(gtfs_var == 'routes'){
  #    out_data <- required_gtfs[['routes']]
  #  }
  #  else if(gtfs_var == 'shapes'){
  #    out_data <- required_gtfs[['shapes']]
  #  }
  #  else if(gtfs_var == 'stop_times'){
  #    out_data <- required_gtfs[['stop_times']]
  #  }
  #  else if(gtfs_var == 'stops'){
  #    out_data <- required_gtfs[['stops']]
  #    out_data <- out_data %>%
  #      dplyr::select(stop_id, stop_lon, stop_lat)
  #    
  #    out_data <- unique(out_data)
  #  }
  #  else if(gtfs_var == 'trips'){
  #    out_data <- required_gtfs[['trips']]
  #  }
  #  else if(gtfs_var == 'services'){
  #    out_data <- required_gtfs[['.']]$dates_services
  #  }
  #  
  #  return(out_data)
  #}
  
  
  #-----------------------------------------------------------------------------
  # stops
  
  #GTFS_stops <- data.frame()
  
  #cl <- makePSOCKcluster(8)
  #registerDoParallel(cl)
  #for(idate in as.character(dates_for_analysis)){
  #  temp_GTFS_stops <- Get_GTFS_by_date(as.Date(idate), 'stops')
  #  GTFS_stops <- rbind(GTFS_stops, temp_GTFS_stops)
  #}
  #stopCluster(cl)
  
  #GTFS_stops <- unique(GTFS_stops)
  
  #return(GTFS_stops)
  #-----------------------------------------------------------------------------
  # trips
  
  #GTFS_trips <- data.frame()
  
  #cl <- makePSOCKcluster(8)
  #registerDoParallel(cl)
  #for(idate in as.character(dates_for_analysis)){
  #  temp_GTFS_trips <- Get_GTFS_by_date(idate, 'trips')
  #  GTFS_trips <- rbind(GTFS_trips, temp_GTFS_trips)
  #}
  #stopCluster(cl)
  
  #GTFS_trips <- unique(GTFS_trips)
  
  #GTFS_trips <- GTFS_trips %>%
  #  filter(route_id == rt, direction_id == di)
  
  ## service_id
  
  #unique_service_id <- unique(GTFS_trips$service_id)
  
  ## shape_id
  
  #unique_shape_id <- unique(GTFS_trips$shape_id)
  
  #-----------------------------------------------------------------------------
  # shapes
  
  #GTFS_shapes <- data.frame()
  
  #cl <- makePSOCKcluster(8)
  #registerDoParallel(cl)
  #for(idate in as.character(dates_for_analysis)){
  #  temp_GTFS_shapes <- Get_GTFS_by_date(idate, 'shapes')
  #  GTFS_shapes <- rbind(GTFS_shapes, temp_GTFS_shapes)
  #}
  #stopCluster(cl)
  
  #GTFS_shapes <- unique(GTFS_shapes)
  
  #GTFS_shapes <- GTFS_shapes %>%
  #  filter(shape_id %in% unique_shape_id)
  
  
  #-----------------------------------------------------------------------------
  # service_id:
  
  #GTFS_services <- data.frame()
  
  #cl <- makePSOCKcluster(8)
  #registerDoParallel(cl)
  #for(idate in as.character(dates_for_analysis)){
  #  temp_GTFS_services <- Get_GTFS_by_date(idate, 'services')
  #  GTFS_services <- rbind(GTFS_services, temp_GTFS_services)
  #}
  #stopCluster(cl)
  
  #GTFS_services <- unique(GTFS_services)
  
  #GTFS_services <- GTFS_services %>%
  #  filter(service_id %in% unique_service_id,
  #         date >= as.Date(min(dates_for_analysis)),
  #         date <= as.Date(max(dates_for_analysis)))
  
  #services_and_trips <- left_join(GTFS_services, GTFS_trips, by = c('service_id'))
  #services_and_trips <- unique(services_and_trips)
  
  #-----------------------------------------------------------------------------
  # stop_times
  
  #GTFS_stop_times <- data.frame()
  
  #cl <- makePSOCKcluster(8)
  #registerDoParallel(cl)
  #for(idate in as.character(dates_for_analysis)){
  #  temp_GTFS_stop_times <- Get_GTFS_by_date(as.Date(idate), 'stop_times')
  #  GTFS_stop_times <- rbind(GTFS_stop_times, temp_GTFS_stop_times)
  #}
  #stopCluster(cl)
  
  #GTFS_stop_times <- GTFS_stop_times %>%
  #  filter(trip_id %in% services_and_trips$trip_id)
  
  
  #services_and_trips_and_stop_times <- left_join(GTFS_stop_times, services_and_trips, by = c('trip_id'))
  #services_and_trips_and_stop_times <- unique(services_and_trips_and_stop_times)
  
  #services_and_trips_and_stop_times <- services_and_trips_and_stop_times %>%
  #  select(trip_id, stop_id, stop_sequence, service_id, route_id, direction_id,
  #         shape_id)
  
  #-----------------------------------------------------------------------------
  # APC data
  
  wego_APC <- read_parquet('Data/wego_2021.parquet')
  
  #wego_APC <- wego_APC %>%
  #  filter(load >= 0)
  
  if(di == '0'){
    di = 'FROM DOWNTOWN'
  }
  else{
    di = 'TO DOWNTOWN'
  }
  
  wego_APC <- wego_APC %>%
    dplyr::filter(route_id == rt, route_direction_name == di, month == mn, type_of_day == service_type,
                  pattern_num == pttn, hour_of_trip == hr)
  
  #wego_APC <- wego_APC %>%
  #  mutate(arrival_time = lubridate::hms(format(arrival_time, format = '%H:%M:%S')) + lubridate::hms('06:00:00'),
  #         time = lubridate::hms(format(time, format = '%H:%M:%S')) + lubridate::hms('06:00:00'),
  #         scheduled_time = hms(format(scheduled_time, format = '%H:%M:%S')) + lubridate::hms('06:00:00'),
  #         transit_date = lubridate::ymd_hms(transit_date) + lubridate::hms('06:00:00'))
  
  #date <- ymd(format(wego_APC$transit_date, format = '%Y-%m-%d'))
  
  #wego_APC$date <- date
  
  #APC_GTFS_merged <- left_join(wego_APC,
  #                             services_and_trips_and_stop_times,
  #                             by = c('trip_id',
  #                                    'stop_id'))
  
  #APC_GTFS_merged <- APC_GTFS_merged[ , -which(names(APC_GTFS_merged) %in% c('stop_sequence.y', 'route_id.y'))]
  #colnames(APC_GTFS_merged)[c(3,5)] <- c('route_id', 'stop_sequence')
  #APC_GTFS_merged <- unique(APC_GTFS_merged)  
  
  #APC_GTFS_merged <- APC_GTFS_merged %>%
  #  filter(service_id == srv_id, shape_id == shp_id)
  
  #file1_name = as.name(paste(paste('APC_and_GTFS_merged', rt, di, mn, service_type, sep = '_'), 'parquet', sep = '.'))
  #write_parquet(APC_GTFS_merged, 'APC_GTFS_merged.parquet')
  
  
  #-----------------------------------------------------------------------------
  
  stops_ref <- wego_APC %>%
    dplyr::select(stop_id, stop_sequence)
  
  stops_ref <- unique(stops_ref)
  
  Main_ons_df <- data.frame()
  
  raw_df_dates <- as.Date(unique(wego_APC$transit_date), format = '%Y-%m-%d %H:%M:%S')
  raw_df_trips <- unique(wego_APC$trip_id)
  raw_df_hours <- unique(wego_APC$hour_of_trip)
  raw_df_windows <- unique(wego_APC$window_of_day)
  
  cl <- makePSOCKcluster(8)
  registerDoParallel(cl)
  for(idate in raw_df_dates){
    for(itrip in raw_df_trips){
      
      temp2_df <- wego_APC %>%
        dplyr::filter(transit_date == idate, trip_id == itrip) %>%
        dplyr::select(transit_date, trip_id, month, hour_of_trip, window_of_day,
                      stop_id, stop_sequence, ons, vehicle_id) %>%
        as.data.frame()
      
      print(head(temp2_df))
      
      if(nrow(temp2_df) > 0){
        missing2_stops <- stops_ref$stop_id[which(stops_ref$stop_id %not_in% temp2_df$stop_id, arr.ind = T)]
        missing2_seq <- stops_ref$stop_sequence[which(stops_ref$stop_id %not_in% temp2_df$stop_id, arr.ind = T)]
        
        print(paste(nrow(temp2_df), length(missing2_stops), sep = ' and '))
        
        if(length(missing2_stops) > 0){
          #imputation2 <- data.frame(
          #  rep(unique(temp2_df$trip_id, length.out = length(missing2_stops))),
          #  rep(unique(temp2_df$month, length.out = length(missing2_stops))),
          #  rep(unique(temp2_df$Hour, length.out = length(missing2_stops))),
          #  missing2_stops, missing2_seq,
          #  NA,
          #  rep(unique(temp2_df$vehicle_id, length.out = length(missing2_stops)))
          #)
          #colnames(imputation2) <- c('trip_id', 'month', 'Hour',
          #                           'stop_id', 'stop_sequence', 'ons',
          #                           'vehicle_id')
          #
          #temp2_df <- rbind(temp2_df, imputation2)
          
          #temp2_trip_id <- unique(temp2_df$trip_id)
          #temp2_stop_ids <- temp2_df$stop_id
          #temp2_stop_seq <- temp2_df$stop_sequence
          #temp2_month <- unique(temp2_df$month)
          #temp2_Hour <- unique(temp2_df$Hour)
          #temp2_arrival_time <- as.character(temp2_df$arrival_time)
          #temp2_ons <- temp2_df$ons
          #temp2_vehicle_id <- unique(temp2_df$vehicle_id)
          
          print('Wait!')
          
        }
        else{
          temp2_date <- unique(as.character(temp2_df$transit_date))
          temp2_trip_id <- unique(temp2_df$trip_id)
          temp2_pattern_num <- unique(temp2_df$pattern_num)
          temp2_stop_ids <- temp2_df$stop_id
          temp2_stop_seq <- temp2_df$stop_sequence
          temp2_month <- unique(temp2_df$month)
          temp2_Hour <- unique(temp2_df$hour_of_trip)
          temp2_arrival_time <- as.character(temp2_df$arrival_time)
          temp2_ons <- temp2_df$ons
          temp2_vehicle_id <- unique(temp2_df$vehicle_id)
        }
        
      }
      else if(nrow(temp2_df) == 0){
        print('There is not any data associated with the given trip_id.')
      }
      ons_out2_df <- data.frame(cbind(temp2_date, temp2_trip_id, temp2_pattern_num,
                                      temp2_month, temp2_Hour,  t(temp2_ons), temp2_vehicle_id))
      
      colnames(ons_out2_df) <- c('date', 'trip_id', 'pattern_num', 'month', 'hour', temp2_stop_ids, 'vehicle_id')
      
      print(ons_out2_df)
      
      if(ncol(ons_out2_df) == 40){
        Main_ons_df <- rbind(Main_ons_df, ons_out2_df)
      }
      
    }
  }
  stopCluster(cl)
  
  Main_ons_df <- unique(Main_ons_df)
  
  if(di == 'FROM DOWNTOWN'){
    di = '0'
  }
  else{
    di = '1'
  }
  
  write_parquet(Main_ons_df, 'Main_ons_df.parquet')
  
  return('Done!')
  
}


# yr, mn, rt, di, service_type, pttn, hr
cl <- makePSOCKcluster(8)
registerDoParallel(cl)
df1 <- get_anomaly_matrix(2021, 1, '56', 'TO DOWNTOWN', 'weekday', 'ROUTE_56_TO DOWNTOWN_3', '9')
stopCluster(cl)




