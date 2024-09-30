FROM apache/airflow:2.8.4-python3.10 as airflow_base
EXPOSE 8080
EXPOSE 8974
WORKDIR /opt/airflow
USER root

# Setting up psycop2g
RUN sudo apt update && \
  sudo apt install -y libpq-dev && \
  sudo rm -rf /var/lib/apt/lists/*


USER airflow

#Installing poetry
#base image has python 3.10 we follow simple installation instructions
RUN pip install poetry==1.8.2
#create a directory for the dags
COPY ["./dags/__init__.py","./dags/healthcare_data_grab_cloud.py","./dags/"]
#copying files for poetry
COPY [ "poetry.lock","pyproject.toml","./"]

#install depenencies
RUN poetry export --without-hashes --format=requirements.txt > requirements.txt
RUN pip install -r requirements.txt
# This approach allows the use of poetry for simple dependency management and solving
# but using pip for the actual installation of the dependencies is closer to 
#airflow's expectations for easier integration with the rest of the workflow

