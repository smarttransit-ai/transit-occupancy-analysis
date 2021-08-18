# Public Transportation Demand

<figure class = "image">
 <img src="Pictures/Inbound_Routes.png" align="right" width="450" height="350" caption="GTFS CARTA Dataset: Inbound Routes."/>
</figure>

This part of the repository contains some Python and R notebooks that were written to model and predict **Public Transportation Demand** (considering board counts only), and **Maximum Occupancy of Trips**. Also, we include a brief explanation and description of the structure and contents of the files and folders. 

Besides that, we explain why the finals models were written in R instead of Python. Moreover, we expect that the information is sufficiently clear so other team members can extend the analysis to for example alight counts or occupancy.

One key pending point is how to automatize model update as new data comes in. For example, the CARTA dataset used for analysis is from 2019-01-02 to 2020-05-31, and new data from GTFS and APC is being processed for further analysis.

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Overview](#overview)
  * [Getting Started](#getting-started)
* [Main Files](#main-files)
  * [Aggregated Performances](#aggregated-performances)
* [Folders and Characteristics](#files-characteristics)
  * [Pictures](#folder1)
  * [Data Example](#data-example)
  * [Related Notebooks](#related-notebooks)
* [Acknowledgements](#acknowledgements)


## Overview

In this part of our repository you will find information about board count modeling at the bus stop level, and maximum occupancy at the trip level. The Models at the bus stop level were trained using data that was grouped by `route_id`, `direction id`, `stop_id`, and data partition. We created a unique folder for each bus stop to store the models and relevant data. The path to these folders follows a hierarchical structure given by their `route_id`, `direction id`, `stop_id`, and data partition. Therefore, we created two notebooks to aggregate the data. 


Similarly, the trip data was grouped by `route_id`, `direction id`, and data partition. We created a unique folder for each trip.v

### Getting Started

R is an open source software that can be downloaded from [R Cran](https://cran.r-project.org/). Also, **R Studio** is the most widely used interpreter, and it can be downloaded from [R Studio](https://rstudio.com/). 

## Main Files

We performed two different data prepartions. One for the bus stop level analysis, and another one for the trip analysis. We modeled transportation demand at the bus stop level by using board counts, and maximum occupancy -grouped by date and hour- for the trip analysis. In both cases, we stored the data based on the hierarchical structures given by the route, direction, and bus stop or trip ID. The following figure shows the data structure for the board count data and models:

<figure class = "image">
 <img src="Pictures/data_structure_routes.png" align="center" width="400" height="350" caption="Bus Stop Data Structure."/>
</figure>

This [link](https://viewer.diagrams.net/?highlight=0000ff&edit=_blank&layers=1&nav=1&title=Data_Structure.drawio#R7V1tc5s4F%2F01%2BRgPQgjQx036sjPbztNpnp22%2ByVDDLGZEpPBpEn661cY8IsuGBskpLDKdKaxDETcoysdnXslXeDrh5ePWfC4%2FJyGUXJhW%2BHLBX53YbMfz2b%2FFSWvZcklpqQsWWRxWJahXcFN%2FDuqCq2q9CkOo%2FXBhXmaJnn8eFg4T1eraJ4flAVZlj4fXnafJod%2F9TFYRKDgZh4ksPRbHObLstQn1q78zyheLOu%2FjKzqm4egvrgqWC%2BDMH3eK8LvL%2FB1lqZ5%2BdvDy3WUFNar7VLe96Hl223FsmiVn3LD3fyj%2Fe3lg%2FX77uWb9%2BmvX3%2B%2Bf366RBUav4LkqXrjz8HL7f%2Fm86fHYDV%2Fvf1%2FVli6fIH8tbZKlj6twqh4sHWBr56XcR7dPAbz4ttn1hBY2TJ%2FSNgnxH69j5PkOk3SbHMvvr%2Bfzyll5es8S39Ge99gF1PMHnoFX616219Rlkcve0XVq36M0ocoz17ZJdW3mFQt77UGxqtweN7BiByvLFvuQehW1wVVy1lsn70zLvulsu85tvaArb9G66ckH2jfRRaEMbPUgY0DYlmNtg9tv8n2oUfvNneIsL1LZuTQ%2BthF0Pq1Z4xjfR9Yn1k5j26R2NYduP7GjsDC1x6uMEkPsLI2P2Isf4lr%2B9WGJ1i54WmL4R2hho%2FwvMXwVxaVbnhMfN3sXv%2BxPbsXnTkr%2BcAGwSiDvc76OX5IglVUmeqm%2BqYw0XwZJ%2BGn4JUBx0rWeTD%2FWX%2B6WqZZ%2FJtdH9TIsK%2BzvBrKbffgipvizuqZWbRm13ypzY64IjYOHVz4KVjndW3SJAke1%2FHdpn7FjQ9BtohXV2mepw%2FVRaDri9z5XHbXd%2BnyDkj9ExsCwtJaAgItAWGbUuiA7C3zJhP59h12XWjUkER%2B6LDyIIkXK1aWRPfFAwqDxYxE%2FVEV52nhvGvmy%2FFq8WlzzTtnV%2FK1skJRlLJ775MNUVrGYRitigaQ5kEe3G1b42Mar%2FKNlcgV%2B8fsdm2xEYewKl2zz2j3mf0rLs%2BY36%2FYWwXxBtmINaXnqGhODZgfd6bullABzxr%2BSbDX14lH3W5GnRjU5aFes051qGOA%2Buw0wFdp2fPvYV0VGZh5mD1fNcyOgVk%2BzMhyVOPsApwBzNEq%2FKNQOnZI7rHkVk4ThQfCB7TB3juThleuy7IoCfL416Fc0mSH6i98KXDfESbHJjNr7wcdsieHck9cp0%2FZPKoesq92nPlcvqaMsi6iHDx3A9rWKANwhHP%2FaeF4iS1LEHQI0a5HyUYLagXnoRW9xPn3oqNjPVj56UfV7RW%2Fv3vZ%2F%2FBafTgVYVaLjSGP1L%2FUOpQ1BUQ9Xv%2FZTofPbQwntCvZjQHqF9NyXdZVCgKr6HSJWrQwnOue6boMlFr5YJxjmS7SVZC835VeMRiz19K7bVJ%2F%2FlHfXnzYOfjm0%2FkeXhpJXw8niHNKz7L6NZlLRqi41ndii2EQBq97l1WE8oxKuwR19D34%2BB3sl7IWYpswnLh%2FyaLbJJ3%2FDNPnFWjOg%2BRqK8BW0KiavvMs6XI1IR7fjlTLphhOoKfV3V%2B6%2FIi6lSjPdl72KL7Dp%2FzgIbnDJ0Px6u7w3wCiHAi%2B1xdRwpMBz5XUHbdW%2Bkhr4%2B%2BwO%2B4Ab8PdIacDJ1CcCeMsmudxuqqTASbRfYNgx7bdjdF9%2F0B%2F59%2Ft%2FPIr%2Feufm0v0%2Fbv7D5t7COm9ASFrsJPuHG1r9%2BEcDUzIpXE0UGmXWMfrZlN69I7BLt7YzBooWrrONeVowKMb2nO7kyMftCPVTi6GonU7uTLfdfiJx4ABnWd7nmvL8V1Y6c7hma%2Bb6OG5sfUcGZ0F5wKpdVyH77XVj84w23BqjmvTmefT3Y97CEH%2FqRbzrpl97Mny5l2NUMLAlMkuOiG7SLBLj5td1NgQYGQLYWxhChrAfzXNpAPydu9qjUuPmVvUWD0YHyswd%2BDk1mAuCvMxM4saqwfDYP%2F1jBPxII%2BZV9Qsn8CkYYOyaJRHTStqhhnKZIa9SWdvtsvHStSztzr6ejCU%2Bxh7Zigf4P9In9zw5vpBBW2Dum9Ql4e6cgaHTNLwCDCr53BQZTMwC4dZAxInJjdcYzXV5gP%2B%2FeVT9qgRM1Wa8RqaA35qporOiPI5eP0DW4gPN0nLVGmt9JGkxnNDYeBtRgmFNSyK1zVTRfDUS30srGFd%2FMS6bywsKw3xA4HHwyLK10GdOx23IylNUv4J1OmmGcSm2vltw2r6qfmtw8WwnUMM%2BrMwTLgYNv%2FkkSlZDZ3RQUtfH0cH5X1aAxm0Ydl8IYj50LWNIHb6FLp0L31l0IZV9BvUsUFdHurKZVDb6GMjwKxcBm3YOsHALBxm9TLo4K0V9tbz9ljOK2apCFFJ%2BItl24jsWLnftnljn%2FXgRx9sjUz3odxl6L5sul%2BzqR3bt5SzfSi9IezYdUq14X39RoqW9W66sH0MNbMN6ibVRSLqytl%2Bw4YghgYKh1k522%2FYNMPALBxm9Wx%2F8PYc0vf30VfYtzFHxdx67fTZxB5xTyKU42qC4nGwyh3rwUHNRlkOjqGQqO%2BWPYPYPPK5dYfbXQXUrQaf%2BqpS5LRNoc7eWtGxYRbUaZ4rzFWGZq29%2FSwohLnDAnyvJ5424p4kKy%2BitcqtLQ3cwB9P0%2FUq3A2SOm4oHk4yAYoXYfym03DG7bWH7rGqu5e7tjC25eAZxp7l%2BS5Brk9sbgiWRb7gGzi0o6LO0Rsk%2BTBU86a6GQ9xOUTUs6960fdk%2FRjzM4r%2BozXP42RlLMMqdw2%2BfM1GGXwdKM1NMonRJtqNvo49fa%2Bd2WjvAACQw9jTiTGyZ96xB488n3KgIGZimrJjmsCjNQhqOlCDQtjBdYqtCW%2F1EsRL99I3qOlA8WuDujnlSyLqyoOajsltGwFm5UFNB6pTBmbhMKsPajomL218Dud4nOCsA4drSkyjLjaJaYP8X%2FPENAIltA3qJjFNIurKORwxiWkjwKycw9XtzMAsE2b1HG7wOUQXmiuqhI%2Fo95ZQCQbnQ46tmjYc0XMeWm8%2FC4XwqXW941ouH26SlYXSWuX2lnZmIAy8yiiBMAJFvElmoYApl%2FpAGJn6JmqeJSrZzOUHAI9wCS2C3BxWuctrO5LNJHntkdyxKYWvXUc%2Fr5168hhzgVl912C%2B5VnezOMfNjbhgsqWkThlS5zAczWQOOs%2BgRO7qNl6esj0uHQvfSVOF4pdG9SpQV0e6solzvqkQaN9yYRZucTpQunLwCwcZvUSpwtFMzaryaNb2IsPme1EeO5vGBCc7ViUSF%2FgiPgzwwk%2BsR%2BVNt1xoT5kyLNs8uzXUzmNtql0oVqFsOO5JlI86LSult23tCHPTUdsMtTNlEki6urJM9S4DKsSDrN68gyFMQOzcJjVk2cP6l4AZrNxTbOAXSy3OuRi23WvZ%2B%2BAgXzuUYTKWYQJK%2B1WKV%2Bn1427Q04cy4Pa3EQ3r6H8si3POpHSS5vaeVAiO69b0N13KX8UUu%2FTI6hFZ87e4RGKj%2FPy5O9Epj224Oybvif8gK2BZS2Nb61ye6s780Qg8CqjHAjkQW1ukilEQJVRfyCQN%2Fn9xzAVRr8YLTrYywbsIyiJjIFX6NpIkNUUHb1Dkh9DhW%2Bqm9kgDE5wVE%2FHhh4moL0vW%2Fwco%2FeYjSye2nl1wFf4VApUumMQhnUbZxQ%2Bcp7mlFICkWXrNwxDIW16rtuQFNjbez16sJGN4ulU7eEmxln69ygxTujHGgQ56xpw4S5zOscggbz0L32DnPVRizzqJrQtEXXlQU7fpI6NALPyIKcP1SkDs3CY1Qc5fZOopoDE2Y5%2BByr7DZlqjmWZ4XxYB6B5pprfkKlWoG4y1SSirp7EmUy1EWBWT%2BJMptoIMKsncRQqcQDmty2kYiB29lZRseXolJRCoXZ2HnZvPymFQSIswoX5uJOstJT2Sh9peefGxMDbjBITo1DYm2RmCpyGqY%2BJ0alvSoYcT1QKGpuogBy00%2FbIONvZYaW7XBfUbRzXPZJUNqlwtoP0c93JZ5UxLxAYznY8R6dwNoX6l1FCpSuhwI81UEJpoyaGbBPOHjKLLv1LXyWUQk1sg7rRvyWirlwJpUYiGwFm5UoosqBEZnAWjrN6KRTVCenTZeGIXxhBeCZ0%2BtJa%2FlHOiQcTd5Nu9jFL03z%2FcjalXH5Ow6i44l8%3D) shows the hierarchical structure for the maximum occupancy data and models.

#### Bus Stop Level

The following notebooks describe how the CARTA data was processed to obtain datasets for modeling purposes. 

* `Data_Preparation.ipynb`: This notebook was written in Python. It shows the initial data partitial into pre- and post-lockdown datasets based on Chattanooga's ridership steep decline, which started the week of March 5th (2020-03-05). Then, it calculates the surrounding board counts by aggregating the board count data from the bus stops that are within a half-mile radius from a referenced bus stop -`radial_influence()`. Finally, it extracts the datasets for modeling purposes -`data_extraction()`.
* `GLM_Demand_Models_Function.ipynb`: This notebook was written in R and shows how to train the models that are based on the Generalized Linear Model theory. It explains how the train and test RMSEs can be calculated using a k-fold cross-validation approach. Its main function -`all_models_cv()`- is able to extract the data for modeling using the `route_id`, `direction_id`, and `stop_id` values. Also, its outputs are stored using the input data values.
* `Rand_Forest_ML.ipynb`: This notebook was written in R and shows how to build our proposed Zero-Inflated Random Forest model. There are two main functions in this notebook. The first function is `RF_Ferns_and_Ranger()` was built to train and tune the model and its parameters. It first learns a classifier to predict counts (> 0) and zero counts using [Random Ferns](https://www.jstatsoft.org/article/view/v061i10/v61i10.pdf), then it takes the *predicted counts* data to learn a regression model for these counts using Random Forest ([Ranger](https://www.jstatsoft.org/article/view/v077i01)). Also, the function is able to extract the data for modeling using the `route_id`, `direction_id`, and `stop_id` values. Also, its outputs are stored using the input data values. Similarly, the second function -`vanilla_rf()`- trains and tunes the vanilla Random Forest model.
* `Rand_Forest_ML2.ipynb`
* `All_GLM_Models_Performance.ipynb`
* `RF_Performance.ipynb`

#### Trip Level

* `Trip_Data_Generation.ipynb`
* `Trip_Level_Modeling_R.ipynb`
* `Trip_Analysis1.ipynb`
* `Trip_Analysis2.ipynb`

Finally, the following notebook shows how to use the `gtfs_functions` package in Python:

* `Carta2018_gtfs_segments`: This notebook explains how to use (or apply) the `gtfs_functions` package using the 2018 gtfs CARTA data. Although, it shows we can reproduce all the processes without problems, we still need to design a way to 'automatize' the gtfs schedules with the data-times of the APC data. Once we solve this, we could simulataneously query (or model) gtfs (using `gtfs_functions`) data and APC data.

### Aggregated Performances

## Folders and Characteristics

### Pictures

It only contains the figures that are used in this readme document.

### Data Example

This folder contains data to run the notebooks in the `Related_Notebooks` folder. The data was extracted from `route_id == '4'`, `direction_id == '0'`, and `stop_id == '12'`.

### Related Notebooks

Our initial modeling approaches are stored in this folder. The Data Example folder has data to run the notebooks in this folder.

## Acknowledgements

This material is based upon work supported by the Department of Energy, Office of Energy Efficiency and Renewable Energy (EERE), under Award Number DE-EE0008467 and National Science Foundation through award numbers 1818901, 1952011, 2029950 and 2029952. The authors will also like to acknowledge the computation resources provided by the Research Computing Data Core at the University of Houston and through cloud research credits provided by Google.
