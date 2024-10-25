# PySpark-Insights-on-CDE-Transaction-Analytics
PySpark Insights on CDE: Transaction Analytics

# Apache Spark Data Engineering Project

## Project Overview

This project demonstrates the use of **Apache Spark** within the **Cloudera Data Engineering (CDE)** platform to process and analyze transaction data. The project guides you through interactive data exploration and batch data engineering using **PySpark**. Core tasks involve reading, transforming, and analyzing transaction and customer datasets to uncover insights and generate meaningful statistics.

## Objectives

- Run PySpark interactive sessions to analyze and transform large datasets.
- Explore data interactively, load structured data from cloud storage, and perform transformations.
- Implement data cleaning, type casting, schema flattening, and data aggregation.
- Perform advanced data engineering tasks such as:
  - Calculating transaction amounts by date and location
  - Finding frequent credit card users and high-transaction customers
  - Calculating transaction distances to identify anomalies in customer spending patterns.

## Setup & Requirements

### Prerequisites
- **Apache Spark** (Using Spark 3.x)
- **Cloudera Data Engineering** access
- **Python** 3.6+
- **PySpark** library (CDE PySpark session)

### Data Sources
- **Transaction Data**: JSON files with nested transaction structures.
- **Customer PII Data**: CSV files containing personally identifiable information.

### Environment Configuration
Navigate to the **Cloudera Data Engineering Home Page**:
1. Launch a PySpark Session (keep default settings).
2. Open the "Interact" tab for code input.
3. Retrieve your assigned storage location and username from the **Trial Manager** homepage.
4. Copy and paste the following variables into the notebook, replacing placeholders with your details:
   ```python
   storageLocation = "<paste-the-trialmanager-configuration-value here>"
   username = "<paste-the-trialmanager-configuration-value here>"
   ```

## Lab Instructions

### Step 1: Load and Explore Transaction Data
Load and print the schema for the transactions dataset:
```python
transactionsDf = spark.read.json("{0}/transactions/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()
```

### Step 2: Flatten Nested Structures and Transform Data
Flatten nested structures in the JSON data and rename columns for ease of use.

### Step 3: Perform Data Analysis
Calculate basic statistics like **Mean** and **Median** transaction amounts:
```python
transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0], 2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0], 2)
print("Transaction Amount Mean: ", transactionsAmountMean)
print("Transaction Amount Median: ", transactionsAmountMedian)
```

### Step 4: Aggregate Data by Time Period
- Group transactions by month and day of the week to analyze spending patterns.

### Step 5: Load and Analyze Customer PII Data
1. Load customer data, cast coordinates to `float`, and create a temporary Spark view.
2. Identify customers with multiple credit cards or addresses.

### Step 6: Join and Compare Datasets
Join the transactions and customer data on credit card numbers to explore spending patterns by location.

### Step 7: Calculate Distance and Identify Anomalous Transactions
1. Implement a PySpark UDF to calculate the distance between the customer’s home and transaction location.
2. Identify customers with transactions occurring more than 100 miles from home.

### Example Code Snippets
- **UDF for Distance Calculation**:
   ```python
   from pyspark.sql.types import FloatType
   distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2) + ((arr[3]-arr[1])**2)**(1/2)), FloatType())
   ```

## Results and Insights
After running the analysis, you will be able to:
- View summary statistics and patterns in transaction data.
- Detect high-value customers and frequent credit card users.
- Identify potentially suspicious transactions based on geographic location data.
