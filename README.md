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
  - Calculating transaction metrics and aggregating data by time periods.
  - Identifying unusual spending patterns based on transaction locations.
  - Integrating customer and transaction data for comprehensive analytics.

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
1. Launch a **CDE PySpark Session**.
2. Open the "Interact" tab for code input.
3. Retrieve your assigned `storageLocation` and `username` from the **Trial Manager**.
4. Paste the values into the notebook:
   ```python
   storageLocation = "<your-storage-location>"
   username = "<your-username>"
   ```

## Project Instructions

### Step 1: Load and Explore Transaction Data
Load the transactions data, displaying the schema to understand the dataset's structure.
```python
transactionsDf = spark.read.json("{0}/transactions/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()
```

### Step 2: Flatten and Transform Data
Flatten nested JSON structures for better accessibility and rename columns to enhance readability.

### Step 3: Key Insights from Transaction Data

- **Transaction Amount Statistics**: Calculated **mean** and **median** transaction amounts to understand the typical spending behavior:
  ```python
  transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0], 2)
  transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0], 2)
  ```
  - **Insight**: Identifying a high average transaction amount suggests potentially high-value customer segments, while the median highlights typical spending patterns.

- **Monthly and Weekly Patterns**: Aggregated transaction amounts by month and day of the week.
  ```python
  spark.sql("SELECT MONTH(event_ts) AS month, avg(transaction_amount) FROM trx GROUP BY month ORDER BY month").show()
  spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, avg(transaction_amount) FROM trx GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()
  ```
  - **Insight**: Recognizing peak transaction days or months can help tailor marketing or resource allocation strategies.

- **Top Credit Card Users**: Listed the most frequently used credit cards to track high-activity customers.
  ```python
  spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM trx GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()
  ```
  - **Insight**: Frequent card usage may indicate valuable or high-risk customers, essential for personalized services or fraud detection.

### Step 4: Load and Explore Customer PII Data

Load and prepare customer data by casting latitude and longitude values to floats for location-based analysis.

### Step 5: Customer Data Insights

- **High Credit Card Ownership**: Found customers with multiple credit cards, highlighting those with high card counts.
  ```python
  spark.sql("SELECT name AS name, COUNT(credit_card_number) AS CC_COUNT FROM cust_info GROUP BY name ORDER BY CC_COUNT DESC LIMIT 100").show()
  ```
  - **Insight**: Customers with multiple cards are ideal candidates for premium or loyalty programs but may also pose a higher fraud risk.

- **Shared Credit Cards**: Identified credit cards used by multiple customers.
  ```python
  spark.sql("SELECT COUNT(name) AS NM_COUNT, credit_card_number AS CC_NUM FROM cust_info GROUP BY CC_NUM ORDER BY NM_COUNT DESC LIMIT 100").show()
  ```
  - **Insight**: Shared credit cards can indicate potential fraud or household usage, requiring further investigation.

- **Multiple Addresses**: Listed customers with multiple addresses.
  ```python
  spark.sql("SELECT name AS name, COUNT(address) AS ADD_COUNT FROM cust_info GROUP BY name ORDER BY ADD_COUNT DESC LIMIT 25").show()
  ```
  - **Insight**: Customers with multiple addresses may indicate frequent movers or, in some cases, potential address mismatches.

### Step 6: Join Datasets for Location-Based Analysis

Joined transaction and customer datasets by credit card numbers, enabling a comparison of transaction locations with customer home coordinates.

### Step 7: Calculate and Analyze Transaction Distances

- **Distance Calculation**: Created a PySpark UDF to calculate the distance between a customer’s registered address and the transaction location.
  ```python
  from pyspark.sql.types import FloatType
  distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2) + ((arr[3]-arr[1])**2)**(1/2)), FloatType())
  distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude", "address_latitude", "address_longitude")))
  ```

- **Anomalous Transactions**: Filtered transactions occurring more than 100 miles from the registered address.
  ```python
  distanceDf.filter(distanceDf.trx_dist_from_home > 100).show()
  ```
  - **Insight**: Identifying customers with significant distances between their transaction and home locations may highlight potential fraudulent activities or unusual behavior.


## Results and Insights
**Insights from Transaction Data**
1. Average Transaction Amounts: We calculate both the mean and median transaction amounts to understand typical spending behaviors. The mean reveals overall trends, while the median provides resilience against the influence of high-value outliers (thanks, VIP customers).

2. Monthly Transaction Patterns: By grouping transactions by month, we uncover spending trends. Does summer drive higher spending? Are there winter savings? This monthly breakdown paints a picture of our customers’ seasonally driven wallet-opening habits.

3. Weekly Spending Habits: Aggregating transactions by day of the week, we identify which days spark the most financial activity. Will we find more transactions on Fridays as people let loose or on Sundays when they stock up for the week ahead?

4. Top Customers by Transaction Volume: We identify our high rollers—the most active customers in terms of transaction counts, possibly setting off our “loyalty program alert.”

5. High-Frequency Credit Card Holders: The data reveal the top 100 customers with multiple credit cards. In some cases, this indicates financial savviness; in others, it might imply caution—or perhaps an affinity for shiny, plastic card collections.

6. Credit Cards Linked to Multiple Names: Discovering credit cards associated with several names can indicate shared cards within families or, let’s be honest, the occasional suspicious activity.

**Insights from Customer PII Data**
Customers with Multiple Addresses: Analyzing address counts shows the top 25 customers who apparently move around—a lot. While some might call them adventurers, you could say they’re just indecisive.

**Insights from Joined Datasets**
- Transaction Location vs. Home Location: By joining transaction data with customer address data, we compare the coordinates of each transaction to the customer’s home address. This tells us if a customer’s transaction occurred within their usual stomping grounds or if they’re spending further afield.

- Transactions Beyond 100 Miles from Home: Using a distance function to measure how far a customer’s transaction is from home reveals cases where the purchase might be worth a second look. Spending over 100 miles away could mean they’re on vacation, but repeated occurrences might suggest a data anomaly (or a seriously long commute).

## Potential Uses
This project’s insights can be useful in multiple scenarios:

1. Anomaly Detection: Identifying out-of-character transactions can aid in fraud detection.
2. Customer Segmentation: Categorizing high-frequency spenders vs. occasional buyers, loyal cardholders, and geographic preferences.
3. Marketing Strategy: Insights into spending habits by day, month, and season offer invaluable clues for tailored marketing campaigns.
4. Each of these insights, crafted by the Spark-PySpark alliance, allows us to take vast amounts of data and distill it into knowledge—like creating a fine reduction sauce from a massive cauldron of data stew.
