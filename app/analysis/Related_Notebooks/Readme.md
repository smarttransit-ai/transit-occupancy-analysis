# Readme

This folder contains some notebooks that were developed to pre-process, analyze and model the transit data.

* `Zero_Inf_RF_PySpark.ipynb`: This notebook uses PySpark and has the a function that that classifies zeros and counts using Random Forest. Then, it performs a regression model for the predicted counts. Besides that, it exports the trained models to a folder that characterizes its dependendent variable (board or alight counts) and its `route_id`, `direction_id`, and `stop_id`.
* `Zero_Infl_Random_Forest.ipynb`: This notebook is an updated version of `Zero_Inf_RF_PySpark.ipynb` with a new feature called `month_average_board_count` which is the The average `board_count` for a given bus stop (`stop_id`) and the time period in the last month. Also, it shows the results using the following `stop_id`s from `route_id == '4'` and `direction_id == '1'`:
  - `stop_id == '12'`.
  - `stop_id == '1351'`.
  - `stop_id == '1883'`. 
* `RF_Pipeline.ipynb`: This notebook is written in R.
* `Demand_Model_Function.ipynb`: This notebook is written in R and contains some functions for data processing and machine learning. These functions are part of a general function that applies machine learning models to board and alight counts. The obtained information is exported as *.csv and *.tex files.
* `Demand_Model_Function_Py.ipynb`: This notebook is the Python version of `Demand_Model_Function.ipynb`. This notebook is not finished yet.
*  `All_Models_Alight.ipynb`:
*  `All_Models_Board.ipynb`:
*  `Smart_Comp.ipynb`: This notebook is written in Python. It describes how to combine -Carta- transit and weather data sets to generate a dataframes for machine learning models. Also, the data is divided in two parts Pre-lockdown and Post-lockdown.
