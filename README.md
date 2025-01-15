# hypercube

**Dockerfile** - used to build image for spark

**start-spark.sh** - starts the Spark master and worker service, binds it to a specific IP address and port, and configures the web UI port. It also writes the console output to a log file for debugging and monitoring purposes.

**docker-compose.yml** - creates spark master and worker containers using image built by Dockerfile

**Folder Structure:**
apps - 

    main.py - Pyspark code to 
                       1. Read - reading raw data (csv & json file)
                       2. Pre-process - deduplication, filtration of invalid data, imputing NULL values
                       3. Transform - joining data, rolling median and aggregation
                       4. Load - loading into database
    
    Modules - Folder contains modules required in main.py, load and transform.
    
    config - contains configuration file for Postgres details
    
    test - Folder contains test scripts

data - input folder for csv files (RawData)

logs - folder to hold all the logs

**Application Readiness:**
1. Create a docker image using "docker build -t hypercube-apache-spark:3.3.1 ."
2. Run docker-compose to create containers docker-compose up -d
3. List the docker containers docker ps
4. To enter a container docker exec -ti <container id> /bin/bash

**Job Execution** - Use below command to trigger a job run in spark master container
/opt/spark/bin/spark-submit --jars /opt/spark-apps/jars/postgresql-42.7.4.jar --master spark://spark-master:7077 /opt/spark-apps/main.py

**Database Access** - Postgres db has been usede to capture the results. This can be accessed using below login details via client like pgadmin4.
HOST - 127.0.0.1
PORT - 5432
DATABASE: postgres
POSTGRES_USER: postgres
POSTGRES_PASSWORD: example

**Test Execution** - Use below command to trigger a job run in spark master container
/opt/spark/bin/spark-submit --jars /opt/spark-apps/jars/postgresql-42.7.4.jar --master spark://spark-master:7077 /opt/spark-apps/test/test_load.py
