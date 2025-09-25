# Polish-Procurement-Data-Processing


## Introduction
The purpose of this project is to build an end to end data engineering pipeline.  It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Python, Apache Spark, S3, and Redshift. Everything is containerized using Docker for ease of deployment and scalability.

- First, polish tender data is brought in from a public API, and immediately loaded into an S3 bucket using Apache Spark. After that job is completed, the raw data from S3 is loaded into another Spark job where it gets put into a JSON schema and cleaned. Specific columns are picked out from the JSON and loaded into Redshift for quicker querying times. 

- This project uses Docker containers to ensure a consistent, reliable, and reproducible environment for data engineering workflows. By containerizing our data pipelines and supporting services, we make the project easier to scale, maintain, and deploy.

- While Docker helps us containerize individual components of our data engineering stack, Kubernetes allows us to orchestrate, scale, and manage those containers efficiently—especially as the project grows in complexity. This project uses Kubernetes to manage the docker containers that contain data pipelines and supporting services. It is also used in scheduling batch jobs using Kubernetes cron jobs. 

- Apache Spark is used to ingest, load, clean, and transform data in this project. Spark is used to ingest data from S3, and load data into S3 and Redshift. 

This is the project plan: 
![alt text](img/system_diagram.png "System Architecture Diagram")
