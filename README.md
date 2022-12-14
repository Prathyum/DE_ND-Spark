## Motivation

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As a data engineer, building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables makes it easier for them to handle their large amounts of data. 
This will allow their analytics team to continue finding insights in what songs their users are listening to.

Additionally, this Spark process is deployed on a cluster using AWS EMR.

## Input Data

The input data contains two S3 buckets:
* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`

The song dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

The log dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

## Tables generated

| Table name | Table type | description | 
| ---- | ---- | ----------- | 
| songplays | Fact table | records in log data associated with song_plays | 
| users | Dimension table | users in the sparkify app | 
| songs | Dimension table | songs in music database | 
| artists | Dimension table |  artists in music database | 
| time | Dimension table | timestamps of records in songplays dismantled into specific units | 

## ETL pipeline

The following steps were taken from FAQ on the [Udacity Knowledge Forum](https://sites.google.com/udacity.com/bosch-talent-ai-scholarship/overview/faqs/data-engineer-projects?authuser=0#h.lvsz9hq3236y) and describe the steps for Linux/Mac users:

### Start Up EMR Cluster:
1. Log into the AWS console for Oregon and navigate to EMR
2. Click "Create Cluster"
3. Select "Go to advanced options"
4. Under "Software Configuration", select Hadoop, Hive, and Spark
   Optional: Select Hue (to view HDFS) and Livy (for running a notebook)
5. Under "Edit software settings", enter the following configuration:
```
[{"classification":"spark", "properties":{"maximizeResourceAllocation":"true"}, "configurations":[]}]
```
6. Click "Next" at the bottom of the page to go to the "Hardware" page
7. I found some EC2 subnets do not work in the Oregon region (where the Udacity S3 data is) -  For example, us-west-2b works fine. us-west-2d does not work (so don't select that)
8. You should only need a couple of worker instances (in addition to the master) - m3.xlarge was sufficient for me when running against the larger song dataset
9. Click "Next" at the bottom of the page to go to the "General Options" page
10. Give your cluster a name and click "Next" at the bottom of the page
11. Pick your EC2 key pair in the drop-down. This is essential if you want to log onto the master node and set up a tunnel to it, etc.
12. Click "Create Cluster"

### Connect to Master Node from a BASH shell and update the spark-env.sh file:
1. On the main page for your cluster in the AWS console, click on SSH next to "Master public DNS"
2. On the Mac/Linux tab, copy the command to ssh into the master node. It should look roughly as follows:
   ssh -i PATH_TO_MY_KEY_PAIR_FILE.pem hadoop@ec2-99-99-999-999.us-west-2.compute.amazonaws.com
3. Paste it and run it in a BASH shell window and type "yes" when prompted, NOTE: You may need to set up an automated SSH ping on your Linux machine to  run every 30 seconds or so to keep the shell connection to EMR alive. 
4. Using sudo, append the following line to the /etc/spark/conf/spark-env.sh file:
```
export PYSPARK_PYTHON=/usr/bin/python3
```
Create a local tunnel to the EMR Spark History Server on your Linux machine:
Open up a new Bash shell and run the following command (using the proper IP for your master node):
```
ssh -i PATH_TO_MY_KEY_PAIR_FILE.pem -N -L 8157:ec2-99-99-999-999.us-west-2.compute.amazonaws.com:18080 hadoop@ec2-99-99-999-999.us-west-2.compute.amazonaws.com
```
NOTE: This establishes a tunnel between your local port 8157 and port 18080 on the master node.
You can pick a different unused number for your local port here.
The list of ports on the EMR side and what UIs they offer can be found at: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html
2. Go to localhost:8157 in a web browser on your local machine and you should see the Spark History Server UI

### Run your job
1. SFTP the dl.cfg and etl.py files to the hadoop account directory on EMR. NOTE: You can SFTP directly from a BASH shell or use an FTP tool (e.g.,  Cyberduck on Mac)
2. In your home directory on the EMR master node (/home/hadoop), run the following command:
```
spark-submit etl.py
```
3. After a couple of minutes your job should show up in the Spark History Server page in your browser.You should see the real-time logging output in your EMR bash shell window as well