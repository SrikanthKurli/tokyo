from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "dd4460e8-26da-48ad-9f74-801c1d80e331",
"fs.azure.account.oauth2.client.secret": 'ucy8Q~hbn7DtNanoZUj3PHmR5WgXc3DVz6TQLceE',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/076baf57-ee47-4e2f-8405-9c86d4c7a511/oauth2/token"}

# dbutils.fs.mount(
# source = "abfss://tokyo-olympic-data@tokyoanalyticsstorage.dfs.core.windows.net", # contrainer@storageacc
# mount_point = "/mnt/tokyoolympicdata",
# extra_configs = configs)

# Create a Spark session
spark = SparkSession.builder.appName("RemoveNewlines").getOrCreate()

# Read the CSV file into a DataFrame
athletes = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/mnt/tokyoolympicdata/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/mnt/tokyoolympicdata/raw-data/coaches.csv")
entiresgender = spark.read.format("csv").option("header","true").option("inferschema","true").load("/mnt/tokyoolympicdata/raw-data/entiresgender.csv")
medals = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/mnt/tokyoolympicdata/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").option("inferschema","true").load("/mnt/tokyoolympicdata/raw-data/teams.csv")

# Data Transformations
athletes = athletes.withColumn("Discipline", regexp_replace("Discipline", "\n", ""))
athletes = athletes.filter(col("PersonName") != "\n")

athletes.show()

medals = medals.withColumn("Rank by Total", regexp_replace("Rank by Total", "\n", ""))
medals = medals.filter(col("Rank") != "\n")
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

entiresgender = entiresgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

entiresgender.show()
coaches.show()
teams.show()

# write data into data lake storage
athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympicdata/transform-data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympicdata/transform-data/coaches")
entiresgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympicdata/transform-data/entiresgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympicdata/transform-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympicdata/transform-data/teams")
