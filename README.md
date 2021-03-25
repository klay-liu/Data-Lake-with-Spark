# PROJECT 04: Data Lake

# 1. Overview

The purpose of this project is to help Sparkify, a music streaming startup, to move the songs and user log data resided in AWS S3 to a data lake, so that the analytics team can find insights in what songs their uses are listening to. In this project, an ETL pipeline that extracts the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables is built. We will deploy this Spark process on a cluster using AWS.

# 2. Data
The data set in this project contains two parts with JSON format resided in S3:

**Song data**: s3://udacity-dend/song_data
The song dataset contains metadata about a song and the artist of that song. 
example data:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

**Log data**: s3://udacity-dend/log_data
The log dataset consists of log files in JSON format. It stores the app activity logs from an imaginary music streaming app based on configuration settings.
example data:
```
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\\"Mozilla\\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\\/537.36 (KHTML, like Gecko) Chrome\\/36.0.1985.143 Safari\\/537.36\\"","userId":"39"}
```


# 3. Technologies
- Python3
- pyspark
- AWS EMR + EC2

# 4. Tables Design

### Fact Table

- **songplays**: 

    Fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


### Dimension Tables

- **users**: 

    Fields: user_id, first_name, last_name, gender, level

- **songs**: 

    Fields: song_id, title, artist_id, year, duration

- **artists**: 

    Fields: artist_id, name, location, latitude, longitude


- **time**: 

    Fields: start_time, hour, day, week, month, year, weekday


# 5. The project files:

`dl.cfg` -  A configuration file containing AWS Credentials(**AWS_ACCESS_KEY, AWS_SECRET_KEY**) used to deploy.

`etl.py` is to load data from S3, process that data with Spark and then write into songs/artists/users/time/songplays tables in S3.

`README.md`  - Description for the project


# 6. ETL Pipeline
- 1. Read data from S3(`s3://udacity-dend/song_data`; `s3://udacity-dend/log_data`.
- 2. Process data using Spark to transform them for fact tables and dimension tables. 
- 3. Load it back to S3
