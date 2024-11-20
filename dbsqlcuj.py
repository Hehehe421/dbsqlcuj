# Databricks notebook source
from pyspark.sql.functions import rand

# Create a random dataset
df = spark.range(0, 1000).withColumn("random_value", rand())

# Split the dataset into training and test sets
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# Display the training and test sets
display(train_df)
display(test_df)
