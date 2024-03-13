# Deployment

Assume all the operations are done in `/code` directory

## 1. Cassandra deployment

There are **two ways** to use the predefined Cassandra Cluster(refer to 1.1 & 1.2)

### 1.1 Use the deployment on Google Cloud Platform

- VM instance IPv4 address: `34.32.201.110` (This maybe change overtime, if not accessiable, please contact <ziqi.wang@aalto.fi>) or refer to [my public repository](https://github.com/James-Leste/Big-Data-Platform-2024)
- Ports

    ```shell
    node1: 9042
    node2: 9043
    node3: 9044
    ```

- Test connections: specify keyspace, IP addresses and port in `connector.py`, run the `main()` function.

    ```python
    def main():
        print(initConnection(keyspace="test", addresses=["34.32.201.110",], port=9042))
    ```

- If you see something like this in the console, it means connection is successful.

    ```shell
    #Successful connection
    Connecting to port 9042
    Created keyspace: test
    Connected successfully !
    <cassandra.cluster.Session object at 0x107695670>
    ```

### 1.2 Use docker compose

If the Google Cloud deployment is down, you can deploy a cluster locally using `docker compose`.

- Make sure docker is properly installed on local machine, you can check by typing `docker -v` in console.
- Start cluster using `docker compose`

    ```shell
    docker compose -f cassandra-compose.yml up -d
    ```

- Use `docker ps` to see if the docker containers are up and running, for example.

    ```shell
    CONTAINER ID   IMAGE              COMMAND                  CREATED             STATUS             PORTS                                                                                    NAMES
    35091d50c2df   cassandra:latest   "docker-entrypoint.s…"   About an hour ago   Up About an hour   7000-7001/tcp, 7199/tcp, 9142/tcp, 9160/tcp, 0.0.0.0:9043->9042/tcp, :::9043->9042/tcp   jamesroot-cassandra2-1
    65c87c92438b   cassandra:latest   "docker-entrypoint.s…"   About an hour ago   Up About an hour   7000-7001/tcp, 7199/tcp, 9142/tcp, 9160/tcp, 0.0.0.0:9044->9042/tcp, :::9044->9042/tcp   jamesroot-cassandra3-1
    4f6a6ef87731   cassandra:latest   "docker-entrypoint.s…"   About an hour ago   Up About an hour   7000-7001/tcp, 7199/tcp, 9142/tcp, 9160/tcp, 0.0.0.0:9042->9042/tcp, :::9042->9042/tcp   jamesroot-cassandra1-1
    ```

- You can follow the instruction in 1.1 from now on. Note that IP address of the cluster is `127.0.0.1` in this case.

## 2. Data ingesting

Dataset used: <https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset>

Dataset directory structure:

```tree
amazon comments
├── [3.4G]  amazon_reviews_multilingual_US_v1_00.tsv
├── [1.8G]  amazon_reviews_us_Apparel_v1_00.tsv
├── [1.3G]  amazon_reviews_us_Automotive_v1_00.tsv
├── [832M]  amazon_reviews_us_Baby_v1_00.tsv
├── [2.0G]  amazon_reviews_us_Beauty_v1_00.tsv
├── [3.0G]  amazon_reviews_us_Books_v1_02.tsv
├── [1.0G]  amazon_reviews_us_Camera_v1_00.tsv
├── [3.0G]  amazon_reviews_us_Digital_Ebook_Purchase_v1_01.tsv
├── [600M]  amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv
├── [ 51M]  amazon_reviews_us_Digital_Software_v1_00.tsv
├── [1.2G]  amazon_reviews_us_Digital_Video_Download_v1_00.tsv
├── [ 70M]  amazon_reviews_us_Digital_Video_Games_v1_00.tsv
├── [1.6G]  amazon_reviews_us_Electronics_v1_00.tsv
├── [350M]  amazon_reviews_us_Furniture_v1_00.tsv
├── [ 38M]  amazon_reviews_us_Gift_Card_v1_00.tsv
├── [912M]  amazon_reviews_us_Grocery_v1_00.tsv
├── [2.3G]  amazon_reviews_us_Health_Personal_Care_v1_00.tsv
├── [ 60M]  amazon_reviews_us_Major_Appliances_v1_00.tsv
├── [1.3G]  amazon_reviews_us_Mobile_Apps_v1_00.tsv
├── [ 56M]  amazon_reviews_us_Mobile_Electronics_v1_00.tsv
├── [3.4G]  amazon_reviews_us_Music_v1_00.tsv
├── [453M]  amazon_reviews_us_Musical_Instruments_v1_00.tsv
├── [1.2G]  amazon_reviews_us_Office_Products_v1_00.tsv
├── [1013M]  amazon_reviews_us_Outdoors_v1_00.tsv
├── [3.4G]  amazon_reviews_us_PC_v1_00.tsv
├── [ 43M]  amazon_reviews_us_Personal_Care_Appliances_v1_00.tsv
├── [1.1G]  amazon_reviews_us_Pet_Products_v1_00.tsv
├── [1.5G]  amazon_reviews_us_Shoes_v1_00.tsv
├── [238M]  amazon_reviews_us_Software_v1_00.tsv
├── [1.9G]  amazon_reviews_us_Sports_v1_00.tsv
├── [752M]  amazon_reviews_us_Tools_v1_00.tsv
├── [1.8G]  amazon_reviews_us_Toys_v1_00.tsv
├── [3.5G]  amazon_reviews_us_Video_DVD_v1_00.tsv
├── [1.1G]  amazon_reviews_us_Video_Games_v1_00.tsv
├── [322M]  amazon_reviews_us_Video_v1_00.tsv
├── [393M]  amazon_reviews_us_Watches_v1_00.tsv
└── [3.9G]  amazon_reviews_us_Wireless_v1_00.tsv
```

Data structure (see `data.tsv` in `/data` folder)

- Change the IP address, port and keyspace in `mysimbdp-dataingest.py`
- Run `pip install -r requirements.txt`
- Run `python3 mysimbdp-dataingest -f /path/to/file`
- You can refer to runtime log `data_ingestion_errors.log` at root directory
