# Amazon Movie Reviews: RDBMS vs NoSQL
This project aims to compare a relational DBMS (PostgreSQL) with a document-based NoSQL database (MongoDB) using a shared dataset.

The goal is to highlight the advantages and limitations of both approaches, focusing on query complexity, performances, indexing strategies and schema flexibility.

**A project made by**
- Angelo Trifelli, 1920939

## Index
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Pre-requisites](#pre-requisites)
- [Execution](#execution)
- [Results](#results)


## Dataset

The dataset that has been selected is the **Amazon movie reviews** dataset (accessible from this [link](https://snap.stanford.edu/data/web-Movies.html)) which covers the period from **1997** to **2012** with almost eight milion reviews. 

## Project Structure

This project is divided as follows:
- **mongodb-config**: this folder contains the initialization file for the MongoDB database ([init.js](/mongodb-config/init.js)) and the queries that have been implemented ([queries-products.js](/mongodb-config/queries-products.js) and [queries-reviews.js](/mongodb-config/queries-reviews.js))
- **postgresql-config**: this folder contains the initialization file for the PostgreSQL database ([init.sql](/postgresql-config/init.sql)) and the queries that have been implemented ([queries.sql](/mongodb-config/queries.js))
- **pgadmin-config**: this folder contains configuration files for the docker container running the PgAdmin application
- **scripts**: this folder contains Python scripts that can be used to process the original dataset and automatically populate both databases.<br />
<u>**Note:**</u> those scripts assume the presence of a folder named **data** containing the raw text file of the original dataset
- **docker-compose**: file containing the configuration for all the docker containers

## Pre-requisites

Before you begin, make sure you have the following pre-requisites in place:
- **Git**: essential for version control and cloning this repository. If you don't have Git installed, you can download it from the official website, based on your operating system: [Download Git](https://git-scm.com/downloads)
- **Python**: used to pre-process the selected dataset and automatically populate both databases. If you don't have python installed on your pc you can download it from the official website: [Download Python](https://www.python.org/downloads/).<br /><br />
Then, you can easily install with **pip** the required dependencies to run the scripts

    ```bash
    pip install psycopg[binary] pymongo tqdm
    ```

- **Docker**: fundamental for creating the containers of the services offered by the application. It's recommended to download **Docker Desktop** from the official website:
  - **Windows**: [Docker Desktop](https://www.docker.com/products/docker-desktop) 


## Execution 
1. **Clone the repository**: open a terminal inside the folder in which you want to download the repository. Then, execute the following **git clone** command to download all the files:

   ```bash
   git clone https://github.com/Angelo-Trifelli/Amazon-Movie-Reviews-RDBMS-vs-NoSQL.git
   ```

2. **Create the containers**: before performing this step make sure that the **docker** service is running on your machine. Open a terminal in the folder containing the cloned repository and run the following command to create all the docker containers: 

   ```bash
   docker-compose up -d 
   ```

3. **Prepare the data folder**: create a new folder named **data** inside the same folder containing the cloned repository. Insert inside the data folder the text file of the original dataset.

4. **Populate the databases**: run the script [populate_rdbms](/scripts/populate_rdbms.py) to automatically populate the PostegreSQL database and the script [populate_nosql](/scripts/populate_nosql.py) to automatically populate the MongoDB database.

5. **Access the databases**: now you can run analytical queries to test both databases. Inside the folders mongodb-config and postgresql-config you can find some queries that have been implemented and tested. To run the queries:
    - **PostgreSQL**: you can use the GUI offered by **PgAdmin**. If the application is already installed on your PC, you can use it directly. Alternatively, this project offers a Docker container running the PgAdmin application, which is accessible via a browser at the following URL 

       ```bash
       http://localhost:5050/
       ```
    - **MongoDB**: you can use the GUI offered by **MongoDB Compass**

## Results
|  Query  | PostgreSQL<br />(no indexes)| PostgreSQL<br />(with indexes)  | MongoDB<br />Product oriented<br />(no indexes) | MongoDB<br />Product oriented<br />(with indexes)   | MongoDB<br />Review oriented<br />(no indexes) | MongoDB<br />Review oriented<br />(with indexes) |
|---------|----------------|----------------|--------------------|---------|-------------------|-----------------|
| QUERY 1 |  4 s 538 ms    |  976 ms        |    1 h 16 minutes  |   N.A   |   55 minutes 21 s |   11 s 244 ms   |        
| QUERY 2 |  7 minutes 47 s|  6 minutes 36 s|    1 h 17 minutes  |   N.A   |   5 h 27 minutes  |   1 h 11 minutes|
| QUERY 3 |  6 s 68 ms     |  120 ms        |    30 s 346 ms     |   N.A   |   10 minutes 51 s |   104 ms        |