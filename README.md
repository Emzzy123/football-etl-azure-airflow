# Football Stadium Data Engineering Pipeline

## Project Overview

This project is an end-to-end data engineering pipeline that extracts football stadium data from Wikipedia, transforms and enriches the data, and saves the final cleaned dataset as a CSV file locally and optionally to Azure Storage.

The project uses Apache Airflow for workflow orchestration, Python for data extraction and transformation, Docker for containerized execution, and Azure Storage as the optional cloud storage destination.

The dataset contains information about association football stadiums by capacity, including stadium name, capacity, region, country, city, home team, image URL, latitude, and longitude.

## Project Objective

The main objective of this project is to demonstrate how a Data Engineer can build a reliable ETL pipeline that:

Extracts data from a web source.

Cleans and transforms raw data.

Enriches the dataset with geographic coordinates.

Orchestrates the pipeline using Apache Airflow.

Runs the full environment using Docker.

Writes the final dataset to a local folder and optionally to Azure Storage.

## Tools and Technologies Used

Python

Apache Airflow

Docker

Docker Compose

Pandas

BeautifulSoup

Requests

Geopy

OpenStreetMap Nominatim

Azure Blob Storage / Azure Data Lake Storage

Git and GitHub

## Project Architecture

The pipeline follows this flow:

```text
Wikipedia Web Page
        |
        v
Python Extraction Script
        |
        v
Apache Airflow DAG
        |
        v
Data Transformation and Cleaning
        |
        v
Location Enrichment using Geopy
        |
        v
Cleaned CSV Output
        |
        v
Local Data Folder / Azure Storage
```

## Folder Structure

```text
FootballDataEngineering/
│
├── dags/
│   └── wikipedia_flow.py
│
├── pipelines/
│   └── wikipedia_pipeline.py
│
├── data/
│   └── .gitkeep
│
├── logs/
│
├── script/
│   └── entrypoint.sh
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
├── .env.example
└── README.md
```

## How the Pipeline Works

The project contains three main Airflow tasks:

```text
extract_data_from_wikipedia
transform_wikipedia_data
write_wikipedia_data
```

### 1. Extract Data from Wikipedia

The first task scrapes the Wikipedia page containing the list of association football stadiums by capacity.

It extracts key columns such as:

Rank

Stadium

Capacity

Region

Country

City

Image URL

Home team

The extracted data is pushed to the next Airflow task using XCom.

### 2. Transform Wikipedia Data

The second task cleans and transforms the extracted data.

This includes:

Converting capacity values to integers.

Cleaning unwanted characters from text.

Replacing missing images with a default image.

Creating latitude and longitude columns.

Enriching the data using city and country values.

The location enrichment is done using Geopy and OpenStreetMap Nominatim.

To improve performance and avoid repeated API calls, the pipeline uses a cache file called:

```text
geocode_cache.json
```

This cache stores successful latitude and longitude results so the same location does not need to be searched again in future runs.

### 3. Write Final Dataset

The final task writes the cleaned data to a CSV file.

The output can be saved locally in the `data` folder and optionally uploaded to Azure Storage.

Example output file:

```text
stadium_cleaned_2026-06-12_13_58_31.csv
```

## Final Dataset Columns

The final cleaned CSV contains the following columns:

```text
rank
stadium
capacity
region
country
city
images
home_team
latitude
longitude
```

## Airflow DAG

The Airflow DAG is defined in:

```text
dags/wikipedia_flow.py
```

The DAG runs manually and follows this task order:

```text
extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data
```

Airflow provides the ability to monitor task success, failure, logs, retries, and execution time.

## Running the Project Locally

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/your-repository-name.git
cd your-repository-name
```

### 2. Create Environment File

Create a `.env` file in the root folder.

Example:

```env
AZURE_STORAGE_KEY=your_azure_storage_key_here
```

If you are only running the project locally and not uploading to Azure, you can keep the Azure upload section disabled.

### 3. Start Docker Containers

```bash
docker compose up -d
```

### 4. Open Airflow UI

Open your browser and go to:

```text
http://localhost:8080
```

Default login:

```text
Username: admin
Password: admin
```

### 5. Trigger the DAG

In Airflow, enable the DAG called:

```text
wikipedia_flow
```

Then manually trigger it.

### 6. Check Output

After the DAG succeeds, check the `data` folder for the generated CSV file.

## Azure Storage Setup

The project can optionally upload the final CSV to Azure Storage.

The Azure path format used is:

```text
abfs://container-name@storage-account-name.dfs.core.windows.net/folder-name/file-name.csv
```

Example:

```text
abfs://footballdataeng@footballdataengsa.dfs.core.windows.net/data/stadium_cleaned.csv
```

The storage account key should not be written directly in the Python code.

It should be stored securely in the `.env` file:

```env
AZURE_STORAGE_KEY=your_azure_storage_key_here
```

The `.env` file must not be pushed to GitHub.

## Data Engineering Concepts Demonstrated

This project demonstrates several important data engineering concepts:

ETL pipeline development.

Workflow orchestration with Apache Airflow.

Containerization with Docker.

Web data extraction.

Data cleaning and transformation.

Data enrichment using external services.

Use of XCom in Airflow.

Local and cloud data storage.

Environment variable management.

GitHub project documentation.

Error handling and debugging.

## Challenges Solved

During the project, several real-world issues were solved:

Wikipedia blocked normal requests, so request headers were added.

The Wikipedia table class changed, so a more flexible table selector was used.

OpenStreetMap geocoding timed out, so caching and safer error handling were added.

Failed geocode results were prevented from being permanently saved as null.

Docker and Airflow dependency issues were fixed.

Azure upload path issues were corrected by separating container name and storage account name.

Secrets were moved from code into environment variables.


## Future Improvements

Possible future improvements include:

Load the cleaned data into Snowflake.

Build a Power BI dashboard using the final CSV.

Add automated scheduling in Airflow.

Add data quality checks.

Add unit tests for transformation logic.

Store pipeline metadata.

Improve geocoding accuracy using a paid API.

Add CI/CD with GitHub Actions.

## Security Notes

Do not push secrets to GitHub.

The following files should remain ignored:

```text
.env
venv/
logs/
__pycache__/
data/*.csv
data/geocode_cache.json
```

If an Azure key is accidentally exposed, rotate the storage account key immediately in Azure Portal.

## Project Status

Completed.

The pipeline successfully extracts football stadium data from Wikipedia, transforms and enriches the dataset, and writes the final cleaned CSV output locally and optionally to Azure Storage.

## Author

Chidoka Emeka
