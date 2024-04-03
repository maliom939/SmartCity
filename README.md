# SmartCity

Developed an ETL pipeline to handle weather, traffic, incident, and camera data throughout a journey. Utilized Kafka for real-time stream processing from various sources, coordinated by Zookeeper, and Spark as a consumer to stream data into S3 buckets in Parquet format.

![Alt text](/pipeline.jpg?raw=true "Title")

AWS Glue was used for automated extraction of data from raw parquet files and creating a data schema for using the transformed data. The data was then loaded into AWS Redshift, serving as a data warehouse for subsequent analytics.

A Power BI dashboard was developed to visualize the travel route alongside various weather and traffic metrics.

![Alt text](/dashboard_image.jpg?raw=true "Title")
