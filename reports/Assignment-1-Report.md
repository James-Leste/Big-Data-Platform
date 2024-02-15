# Assignment Report

## Design

### 1. Data, Domain,& Distribute Database

#### Selections

For this project, <a href="https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset">Amazon Customer Reviews</a> and <a href="https://cassandra.apache.org/">Cassandra</a> are selected.  

#### Cassandra

> Apache Cassandra is a database that focuses on reliable performance, speed and scalability. It quickly stores massive amounts of incoming data and can handle hundreds of thousands of writes per second. <a href="https://ubuntu.com/blog/apache-cassandra-top-benefits">source</a>

#### Amazon Customer Reviews

Data structure can be inspected in `data.tsv` in `/data` folder.

> Amazon Customer Reviews (a.k.a. Product Reviews) is one of Amazon’s iconic products. In a period of over two decades since the first review in 1995, millions of Amazon customers have contributed over a hundred million reviews to express opinions and describe their experiences regarding products on the Amazon.com website. This makes Amazon Customer Reviews a rich source of information for academic researchers in the fields of Natural Language Processing (NLP), Information Retrieval (IR), and Machine Learning (ML), amongst others. <a href="https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset">source</a>

Dataset directory structure:

```tree
amazon reviews [22G]
├── [3.4G]  amazon_reviews_multilingual_US_v1_00.tsv
├── [1.8G]  amazon_reviews_us_Apparel_v1_00.tsv
├── [1.3G]  amazon_reviews_us_Automotive_v1_00.tsv
...
```

#### Data source & assumption

In the project, since there is no real dataflow from business environment, the data comes from a local disk. Ingestion of data happens on multiple threads.
I assume that, in a real business environment, dataflow is dynamic and comes from millions of users that operate on the platform concurrently.  

Situations/assumptions where the platform serves for big data workload:

1. Batch data are ingested for model training or business analysis.
2. Reading and recording millions customers' reviews synchronously.
3. Data backup/migration.

#### Reasons for selecting Amazon Reviews

1. Its big enough(more than 22GB) to demonstrate the workflow of a big data platform.
2. The dataset contains multiple columns (with various data types) and is rather clean and standardized so that it don't need excessive data cleaning.
3. Semantically, the dataset is useful in many domains, e.g. Large Language Model (LLM) trainning, business analysis, etc.

#### Reasons for selecting Cassandra

1. Cassandra is highly scalable and one can increase performance just by adding a new rack.
2. There is no “master” that needs to be super-sized to handle orchestrating and managing data, more economical.
3. Data can be quickly replicated across the entire system, regardless of geographic location. It ensures data integrity.
4. If a particular node fails, users will be automatically moved to the closest working node.

#### Data types

Data types majorly contains `INTEGER`, `TEXT`. Specially, `review_date` will be converted to `Date` type in the database.

```shell
marketplace: <class 'str'>
customer_id: <class 'int'>
product_id: <class 'str'>
product_parent: <class 'int'>
product_title: <class 'str'>
product_category: <class 'str'>
star_rating: <class 'int'>
helpful_votes: <class 'int'>
total_votes: <class 'int'>
vine: <class 'int'>
verified_purchase: <class 'str'>
review_headline: <class 'str'>
review_body: <class 'str'>
review_date: <class 'str'>
```




## Implementation

## Source code structure

- `cassandra-compose.yml`: contains cassandra cluster docker image configuration.
- `connector.py`: contains `createKeyspace()`, `initDatabase()` and `initinitConnection()` functions for database connection.
- `mysimbdp-dataingest.py`: contains `ingesting()` for data ingestion.
- `requirements.txt`: contains python package requirements.
