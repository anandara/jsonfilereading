// Databricks notebook source
val mdf = spark.read.option("multiline", "true").json("/FileStore/tables/test_json.json")
mdf.show(false)

// COMMAND ----------

mdf.createOrReplaceTempView("Jsonread")

// COMMAND ----------

val results = sqlContext.sql("SELECT ppu FROM Jsonread ")


// COMMAND ----------

results.show(false)

// COMMAND ----------


