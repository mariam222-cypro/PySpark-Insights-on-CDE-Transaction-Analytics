
---

### A Brief Introduction to Spark

Apache Spark is an open-source, distributed processing system used for big data workloads. It has gained extreme popularity as the go-to engine for interactive Data Analysis and the deployment of Production Data Engineering and Machine Learning pipelines at scale.

In CDE (Cloudera Data Engineering) you can use Spark to explore data interactively via CDE Sessions or deploy batch data engineering pipelines via CDE Jobs.


```
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

storageLocation = "<paste-the-trialmanager-configuration-value here>"
username = "<paste-the-trialmanager-configuration-value here>"
```


```
### LOAD HISTORICAL TRANSACTIONS FILE FROM CLOUD STORAGE
transactionsDf = spark.read.json("{0}/transactions/{1}/rawtransactions".format(storageLocation, username))
transactionsDf.printSchema()
```

```
### CREATE PYTHON FUNCTION TO FLATTEN PYSPARK DATAFRAME NESTED STRUCTS
def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(F.col(prefix + elem.name).alias(prefix + elem.name))
    return result
```

```
### RUN PYTHON FUNCTION TO FLATTEN NESTED STRUCTS AND VALIDATE NEW SCHEMA
transactionsDf = transactionsDf.select(flatten_struct(transactionsDf.schema))
transactionsDf.printSchema()
```

```
### RENAME COLUMNS
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_amount", "transaction_amount")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_currency", "transaction_currency")
transactionsDf = transactionsDf.withColumnRenamed("transaction.transaction_type", "transaction_type")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.latitude", "latitude")
transactionsDf = transactionsDf.withColumnRenamed("transaction_geolocation.longitude", "longitude")
```

```
### CAST COLUMN TYPES FROM STRING TO APPROPRIATE TYPE
transactionsDf = transactionsDf.withColumn("transaction_amount",  transactionsDf["transaction_amount"].cast('float'))
transactionsDf = transactionsDf.withColumn("latitude",  transactionsDf["latitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("longitude",  transactionsDf["longitude"].cast('float'))
transactionsDf = transactionsDf.withColumn("event_ts", transactionsDf["event_ts"].cast("timestamp"))
```

```
### CALCULATE MEAN AND MEDIAN CREDIT CARD TRANSACTION AMOUNT
transactionsAmountMean = round(transactionsDf.select(F.mean("transaction_amount")).collect()[0][0],2)
transactionsAmountMedian = round(transactionsDf.stat.approxQuantile("transaction_amount", [0.5], 0.001)[0],2)

print("Transaction Amount Mean: ", transactionsAmountMean)
print("Transaction Amount Median: ", transactionsAmountMedian)
```

```
### CREATE SPARK TEMPORARY VIEW FROM DATAFRAME
transactionsDf.createOrReplaceTempView("trx")
spark.sql("SELECT * FROM trx LIMIT 10").show()
```

```
### CALCULATE AVERAGE TRANSACTION AMOUNT BY MONTH
spark.sql("SELECT MONTH(event_ts) AS month, \
          avg(transaction_amount) FROM trx GROUP BY month ORDER BY month").show()
```

```
### CALCULATE AVERAGE TRANSACTION AMOUNT BY DAY OF WEEK
spark.sql("SELECT DAYOFWEEK(event_ts) AS DAYOFWEEK, \
          avg(transaction_amount) FROM trx GROUP BY DAYOFWEEK ORDER BY DAYOFWEEK").show()
```

```
### CALCULATE NUMBER OF TRANSACTIONS BY CREDIT CARD
spark.sql("SELECT CREDIT_CARD_NUMBER, COUNT(*) AS COUNT FROM trx \
            GROUP BY CREDIT_CARD_NUMBER ORDER BY COUNT DESC LIMIT 10").show()
```

```
### LOAD CUSTOMER PII DATA FROM CLOUD STORAGE
piiDf = spark.read.options(header='True', delimiter=',').csv("{0}/pii/{1}/pii".format(storageLocation, username))
piiDf.show()
piiDf.printSchema()
```

```
### CAST LAT LON TO FLOAT TYPE AND CREATE TEMPORARY VIEW
piiDf = piiDf.withColumn("address_latitude",  piiDf["address_latitude"].cast('float'))
piiDf = piiDf.withColumn("address_longitude",  piiDf["address_longitude"].cast('float'))
piiDf.createOrReplaceTempView("cust_info")
```

```
### SELECT TOP 100 CUSTOMERS WITH MULTIPLE CREDIT CARDS SORTED BY NUMBER OF CREDIT CARDS FROM HIGHEST TO LOWEST
spark.sql("SELECT name AS name, \
          COUNT(credit_card_number) AS CC_COUNT FROM cust_info GROUP BY name ORDER BY CC_COUNT DESC \
          LIMIT 100").show()
```

```
### SELECT TOP 100 CREDIT CARDS WITH MULTIPLE NAMES SORTED FROM HIGHEST TO LOWEST
spark.sql("SELECT COUNT(name) AS NM_COUNT, \
          credit_card_number AS CC_NUM FROM cust_info GROUP BY CC_NUM ORDER BY NM_COUNT DESC \
          LIMIT 100").show()
```

```
# SELECT TOP 25 CUSTOMERS WITH MULTIPLE ADDRESSES SORTED FROM HIGHEST TO LOWEST
spark.sql("SELECT name AS name, \
          COUNT(address) AS ADD_COUNT FROM cust_info GROUP BY name ORDER BY ADD_COUNT DESC \
          LIMIT 25").show()
```

```
### JOIN DATASETS AND COMPARE CREDIT CARD OWNER COORDINATES WITH TRANSACTION COORDINATES
joinDf = spark.sql("""SELECT i.name, i.address_longitude, i.address_latitude, i.bank_country,
          r.credit_card_provider, r.event_ts, r.transaction_amount, r.longitude, r.latitude
          FROM cust_info i INNER JOIN trx r
          ON i.credit_card_number == r.credit_card_number;""")
joinDf.show()
```

```
### CREATE PYSPARK UDF TO CALCULATE DISTANCE BETWEEN TRANSACTION AND HOME LOCATIONS
distanceFunc = F.udf(lambda arr: (((arr[2]-arr[0])**2)+((arr[3]-arr[1])**2)**(1/2)), FloatType())
distanceDf = joinDf.withColumn("trx_dist_from_home", distanceFunc(F.array("latitude", "longitude",
                                                                            "address_latitude", "address_longitude")))
```

```
### SELECT CUSTOMERS WHOSE TRANSACTION OCCURRED MORE THAN 100 MILES FROM HOME
distanceDf.filter(distanceDf.trx_dist_from_home > 100).show()
```
---
