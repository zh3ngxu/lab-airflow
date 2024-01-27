# lab-airflow
A project to create end-to-end pipeline using Apache Airflow


## Summary
Implement a workflow to detect raw sales data landing in S3 bucket from vendor and process using Spark in clusters in AWS EMR service, the output parquet files will be loaded into Snowflake data warehouse for queries. 


## Set up environment
create EC2 with Ubuntu image. Make sure to use large instance as there's minimum memory or CPU cores required.

Set up IAM role or instance profile and attach with the EC2 instance. Make sure the S3FullAccess policy is included in the role.

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

Use Dockerfile to install dependecy in a custom image, and comment out `image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.1} # build: .` in docker-compose yaml file.


## load files preparation
The `sales.csv` has sample product sales dataset, use the `file_split.py` to create files containing sales events on each day. And create a S3 bucket to upload files to s3 bucket. This step is to mimic the vendor sales data sharing, and detecting and picking files for process will be the first step in the pipeline

```bash
cd sales_data/
pip3 install pandas
python3 file_split.py

export mybucket="wcddeb8-lab-airflow-yourname"
aws s3 mb s3://$mybucket
aws s3 mb s3://$mybucket-output

chmod +x upload_raw_files.sh
./upload_raw_files.sh
```


