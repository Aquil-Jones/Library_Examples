# Interview Aquil


## Introduction & Quickstart

Hello this github repo brings in the [COVID-19 Reported Patient Impact and Hospital Capacity by State Timeseries](https://healthdata.gov/dataset/COVID-19-Reported-Patient-Impact-and-Hospital-Capa/qqte-vkut/data_preview). As a use case has not been generated for this data yet I have decided to collect a few columns and perform some basic data cleaning, perform a few operations to give columns that data scientist might like and finally add some of the metadata to the file directly before exporting to parquet. More on all that below.
If you just want to jump in getting this repo up and running is simple, run the following commands from the top directrory of this repo.
```
docker compose build
docker compose up airflow-init
docker compose up
```
Give a few minutes for the webserver to launch and navigate to ```localhost:8080```
from there enter the username and password of *airflow* to access the webserver and run the dag *healthcare_data_grab_cloud*
As far as cleanup the following commands should suffice
```docker compose down --volumes --remove-orphans```
If you wish to make changes to any of the files after running the docker compose commands you will have to change their ownership the command for that is: 
```sudo chown -R <your_username> .```

To run the examples in the [Examples_Notebook](./Examples_Notebook.ipynb) you simply need to install [poetry](https://python-poetry.org/docs/#installing-with-pipx) and run poetry install this will create a virtualenv with the proper dependencies and you can proceed from there.

## Rationale

<p align="center"> In this section I will go over some of my reasoning behind the code</p>

### Why use Poetry?

Dependency management in python is hard and pip makes handling the complexity of modern deployment unneccessarily difficult<sup>[1](https://www.reddit.com/r/Python/comments/uxcsa1/poetry_over_pip/)</sup> <sup>[2](https://medium.com/@utkarshshukla.author/managing-python-dependencies-the-battle-between-pip-and-poetry-d12e58c9c168)</sup> <sup>[3](https://news.ycombinator.com/item?id=38844798)</sup>. While all of the links above tell that story I actually think for a project of this size pip is the better tool. So why use poetry? Simple part of the goal of this project is to show off and my final Dockerfile makes use of both allowing me the convienence of poetry in my regular development workflow while still showcasing both tools.

### Why this dataset?

The COVID-19 pandemic was a global disaster that will affect human history and memory for years to come. This dataset at least allows a way of making some sense of the impact this has had on the medical system in the US if not on people as a whole. In addition the data is clean enough. There are errors for sure but this is not a dataset that I can say needs heavy cleaning allowing me to add some value via transformations.

### Why not more SQL?

The vast majority of the data transformations I used in this project could be done in a modern data warehouse such as Redshift. This would essentially change the pattern from one of ETL to ELT. With the use of dbt this may be the preffered flow however getting that to all work in a local contect was too much for my machine. If so desired I can affect similar workflows by making more use of the sparkSQL engine which I use a little bit og in the examples notebook. 

### Why S3?

This data is clean enough and the the columns are named explicitly I would be comfortable putting it in a SQL database if that were asked. However the full use case for this data is still unknown loading in S3 allows the flexibility to take to any number of database systems and still have Athena in AWS for quick querying.

### Why not Pyspark?
The simple answer here is size. Pyspark is great for large workflows but this dataset is not even a whole gigabyte in size(36.5 MB) when downloaded as a csv. Pyspark would be overkill.
