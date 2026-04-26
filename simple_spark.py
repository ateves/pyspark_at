from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize a SparkSession
spark = SparkSession.builder \
    .appName("SimplePySparkApp") \
    .getOrCreate()

# 2. Create sample data
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29), ("David", 22)]
columns = ["Name", "Age"]

# 3. Create a DataFrame
df = spark.createDataFrame(data, columns)

# 4. Perform transformations (Filter and Select)
# We filter for people older than 30 and only keep their names
filtered_df = df.filter(col("Age") > 30).select("Name")

# 5. Show the result (Action)
print("Users older than 30:")
filtered_df.show()

# 6. Stop the session
spark.stop()
