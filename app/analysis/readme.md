<img src="Logo/2.png" align="right" width="350" height="350"/>

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Overview](#overview)
  * [Restricted Use](#restricted-use)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)
* [Acknowledgements](#acknowledgements)




# Files and Characteristics

This folder is where the analysis and results are kept. 

* `Data_Preparation.ipynb`: This notebook is written in python and prepares the data (in this case the CARTA dataset) for regression. Part of its main function is export the prepared data as `*.csv` to a folder that characterizes its dependendent variable (board or alight counts) and its `route_id`, `direction_id`, and `stop_id`.
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
*  `Carta2018_gtfs_segments`: This notebook explains how to use (or apply) the `gtfs_functions` package using the 2018 gtfs CARTA data. Although, it shows we can reproduce all the processes without problems, we still need to design a way to 'automatize' the gtfs schedules with the data-times of the APC data. Once we solve this, we could simulataneously query (or model) gtfs (using `gtfs_functions`) data and APC data.
*  `Data_Example`: This folder has some data that can be used to run the notebooks.
* `Rand_Forest_L.ipynb`: This notebook contains the Random Forest based models at the bus stop level. This code was written in R. There two models in this notebook: Zero-Inflated Random Forest and (Vanilla) Random Forest.
* `Trip_Analysis1.ipynb`: This notebook contains the Random Forest based models at the trip stop level. This code was written in R.
