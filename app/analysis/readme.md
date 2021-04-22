# Files and Characteristics

This folder is where the analysis and results are kept. 

* `Demand_Model_Function.ipynb`: This notebook is written in R and contains some functions for data processing and machine learning. These functions are part of a general function that applies machine learning models to board and alight counts. The obtained information is exported as *.csv and *.tex files.
* `Demand_Model_Function_Py.ipynb`: This notebook is the Python version of `Demand_Model_Function.ipynb`. This notebook is not finished yet.
*  `All_Models_Alight.ipynb`:
*  `All_Models_Board.ipynb`:
*  `Smart_Comp.ipynb`: This notebook is written in Python. It describes how to combine -Carta- transit and weather data sets to generate a dataframes for machine learning models. Also, the data is divided in two parts Pre-lockdown and Post-lockdown.
*  `Carta2018_gtfs_segments`: This notebook explains how to use (or apply) the `gtfs_functions` package using the 2018 gtfs CARTA data. Although, it shows we can reproduce all the processes without problems, we still need to design a way to 'automatize' the gtfs schedules with the data-times of the APC data. Once we solve this, we could simulataneously query (or model) gtfs (using `gtfs_functions`) data and APC data.
*  `Data_Example`: This folder has some data that can be used to run the notebooks.

