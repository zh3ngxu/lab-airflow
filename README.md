# lab-airflow
A project to create end-to-end pipeline using Apache Airflow


## Summary
Implement a workflow to detect raw sales data landing in S3 bucket from vendor and process using Spark in clusters in AWS EMR service, the output parquet files will be loaded into Snowflake data warehouse for queries. 

## prerequisite
- Snowflake free trail account(deployed on AWS preferred)
- AWS account


## Set up environment
create EC2 with Ubuntu image. Make sure to use large instance as there's minimum memory or CPU cores required.

Set up IAM role or instance profile and attach with the EC2 instance. Make sure the `AmazonS3FullAccess` and `AmazonEMRFullAccessPolicy_v2` policy are included in the role.

Once connected to EC2 instance, copy the project folder into the EC2 and install dependencies. Execute the following command to make the shell script executable and install pip, docker and aws command line tool.

```bash
chmod +x ./ubuntu_sys_init.sh
sudo ./ubuntu_sys_init.sh
```


## install airflow

[reference doc](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up airflow-init
docker-compose up
```

If you would like to customize the Docker image, please modify the Dockerfile and uncomment line 53: `image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.1} # build: .` in docker-compose yaml file.


## load files preparation
The `sales.csv` has sample product sales dataset, use the `file_split.py` to create files containing sales events on each day. And create a S3 bucket to upload files to s3 bucket. This step is to mimic the sales data dropping from vendors, and the file existance will be validated in the first task of the workflow. Update the `s3_bucket` in the dag file as well.

```bash
cd sales_data/
pip3 install pandas
python3 file_split.py

export mybucket="wcddeb8-lab-airflow-yourname"
aws s3 mb s3://$mybucket

chmod +x upload_raw_files.sh
./upload_raw_files.sh
```

## create dag file

create a dag folder and the `mydag.py` under the project directory. The file contains all tasks to orchestrate a workflow to ingest raw files from an S3 bucket, process them using Spark in AWS EMR, and load the processed data into a Snowflake data warehouse.

#### Tasks Overview
**S3 List Operator (list_objects_task):**

Task to list objects in an S3 bucket with a specified prefix.
The result is stored in XCom, which is used in the subsequent task to determine whether files exist.

**Branch Python Operator (check_s3_file_exist):**
A Python callable (_branch_func) is executed to check if files exist based on the information retrieved in the previous task.
If files exist, it branches to the task emr_process_file; otherwise, it goes to notify_vendor.

**Python Operator (notify_vendor):**
Placeholder task to notify vendors about missing files

**Bash Operator (emr_process_file):**
Placeholder task to echo "processing files"

**EMR Add Steps Operator (add_step_job):**
Adds Spark steps to an existing EMR cluster (CLUSTER_ID) for processing data.
The Spark steps are defined in the `SPARK_STEPS` variable.

**EMR Step Sensor (wait_for_step_job):**
Waits for the Spark job added in the previous step to complete before proceeding.

**Snowflake Operator (load_snowflake):**
Loads data into Snowflake using a SQL script (sf_load.sql) in the sales_db database and sales_schema schema.


## Set up EMR clister
Create EMR cluster in AWS management console, the version could be `emr-6.15.0` with application `Hadoop Hive Spark`installed, make sure `EC2 instance profile` is attached with an IAM role that has `AmazonS3FullAccess` policy so that EMR cluster could read files from S3 bucket and write parquet files back to the bucket. Service role for Amazon EMR could be set to `EMR_DefaultRole`. Wait for around 10 minutes and copy the cluster id and replace `CLUSTER_ID` in the dag file.

## Set up Snowflake
Login to the Snowflake website, and create a new worksheet as the default `ACCOUNTADMIN` role and copy the `scripts/snowflake_init.sql` code in the worksheet and execute the query.


## set up airflow connections
In Airflow web UI, click on `Admin/Connections` to set up two connections:
- `aws_default` with `aws` connection type, set `extra` field with the json string `{"region_name": "us-east-1"}`.
- `snowflake_default` with `snowflake` connection type. Fill out the following fields:

  - login: `your username`
  - password: `your password`
  - account: account can be fetched within the bottom left button in the Snowsight UI, expand the menu and click on the icon "copy account URL", - here's an example value: `uu78372.ca-central-1.aws`
  - warehouse: `etl_wh`
  - database: `sales_db`
  - role: `accountadmin`

Finally Start the dag by clicking on unpause toggle button and check the dag run status.

Next steps:
- Add a task to automate the EMR cluster creation using the [EmrCreateJobFlowOperator](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/emr/emr.html#create-an-emr-job-flow)
- 