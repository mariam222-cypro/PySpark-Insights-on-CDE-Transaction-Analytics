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
