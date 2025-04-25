# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Run DLT pipeline with data quality checks
# MAGIC
# MAGIC The DLT notebooks for producing the output data has already been developed.
# MAGIC Your next task is to run the notebooks yourself.
# MAGIC
# MAGIC To run it, you shall now to manually setup a pipeline run.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## In and out data
# MAGIC
# MAGIC ### Incoming data
# MAGIC
# MAGIC - A sample of NYC Taxi data, from Unity Catalog (UC) path `training.taxinyc_trips.yellow_taxi_trips_curated_sample`
# MAGIC - NYC population data
# MAGIC
# MAGIC ### Outgoing data product
# MAGIC
# MAGIC We produce a data product called revenue. For now it contains the following outgoing data sets:
# MAGIC
# MAGIC 1. `revenue_by_tripmonth`
# MAGIC 2. `revenue_by_borough`
# MAGIC 3. `revenue_per_inhabitant`
# MAGIC
# MAGIC `borough_population` is an intermediate internal dataset.
# MAGIC
# MAGIC In production, the data product will be written as the UC schema `acme_transport_taxinyc.dltrevenue_w_dq`.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Task: Set up at DLT pipeline run.
# MAGIC
# MAGIC For each notebook, connect to the UC Shared Cluster you have been assigned, and run through the cells. Study the cells as you run them.
# MAGIC
# MAGIC ### Setup the database schema where we will write data
# MAGIC
# MAGIC 1. Go to `orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue_w_dq/setupdb`
# MAGIC 2. Run the notebook
# MAGIC 3. Copy the database name, which will be something like `dev_paldevibe_dataopsv2_e88409f3_dltrevenue_w_dq`
# MAGIC
# MAGIC ### Run the DLT pipeline
# MAGIC
# MAGIC 3. Go to the Delta Live Tables menu under Data Engineering on the right side menu
# MAGIC 4. Press `Create pipeline`
# MAGIC     1. `Pipeline name`: `dltrevenue_w_dq_[your username]_manual_test`
# MAGIC     2. `Product edition`: `Advanced`
# MAGIC     2. `Source code`: Lookup the `orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue_w_dq/revenue_w_dq` notebook
# MAGIC     3. `Destination`: Unity Catalog
# MAGIC     4. `Catalog`: `acme_transport_taxinyc`
# MAGIC     5. `Target schema`: Select the name of the schema/database you created in the setupdb notebook
# MAGIC     6. `Compute -> Cluster Policy`: `dlt_default_policy`. If you cannot select a policy, save and open again to edit.
# MAGIC     7. Don't add any other options, or set any policies
# MAGIC 5. Press `Start` to run the pipeline. It can take 5-10m for the first run. Later runs are faster.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Investigate why the pipeline fails
# MAGIC
# MAGIC The pipeline should fail in the `revenue_by_borough` step. Why did it fail?

# COMMAND ----------

# MAGIC %md 
# MAGIC _Add reason for the failure here..._  
# MAGIC   
# MAGIC com.databricks.pipelines.common.errors.DLTSparkException: [EXPECTATION_VIOLATION.VERBOSITY_ALL] Flow 'acme_transport_taxinyc.dev_selimozcan_featuresotraining_3a6901d6_dltrevenue_w_dq.revenue_by_borough' failed to meet the expectation. Violated expectations: 'pickup_borough_not_null, pickup_borough_not_unknown'. Input data: 'Input could not be determined from the violating output record.'. Output record: '{"pickup_borough":null,"amount":1.1202105057157028E8}'. Missing input data: true
# MAGIC at com.databricks.pipelines.common.errors.formatting.ErrorFormatter$.expectationViolationToDLTSparkException(ErrorFormatter_Spark.scala:76)
# MAGIC at com.databricks.pipelines.common.errors.formatting.ErrorFormatter$.format(ErrorFormatter_Spark.scala:43)
# MAGIC at com.databricks.pipelines.execution.core.PhysicalFlow.$anonfun$executeAsync$2(PhysicalFlow.scala:201)
# MAGIC at com.databricks.common.util.InstrumentedFuture.$anonfun$transform$2(InstrumentedFuture.scala:44)
# MAGIC at com.databricks.logging.AttributionContextTracing.$anonfun$withAttributionContext$1(AttributionContextTracing.scala:48)
# MAGIC at com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:276)
# MAGIC at scala.util.DynamicVariable.withValue(DynamicVariable.scala:62)
# MAGIC at com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:272)
# MAGIC at com.databricks.logging.AttributionContextTracing.withAttributionContext(AttributionContextTracing.scala:46)
# MAGIC at com.databricks.logging.AttributionContextTracing.withAttributionContext$(AttributionContextTracing.scala:43)
# MAGIC at com.databricks.common.util.InstrumentedFuture.withAttributionContext(InstrumentedFuture.scala:18)
# MAGIC at com.databricks.logging.AttributionContextTracing.withAttributionTags(AttributionContextTracing.scala:95)
# MAGIC at com.databricks.logging.AttributionContextTracing.withAttributionTags$(AttributionContextTracing.scala:76)
# MAGIC at com.databricks.common.util.InstrumentedFuture.withAttributionTags(InstrumentedFuture.scala:18)
# MAGIC at com.databricks.common.util.InstrumentedFuture.$anonfun$transform$1(InstrumentedFuture.scala:44)
# MAGIC at scala.concurrent.impl.Promise.liftedTree1$1(Promise.scala:46)
# MAGIC at scala.concurrent.impl.Promise.$anonfun$transform$1(Promise.scala:46)
# MAGIC at scala.concurrent.impl.CallbackRunnable.run(Promise.scala:77)
# MAGIC at com.databricks.pipelines.execution.core.PhysicalFlow$$anon$1$$anon$2.$anonfun$run$1(PhysicalFlow.scala:305)
# MAGIC at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
# MAGIC at com.databricks.logging.AttributionContext$.$anonfun$withValue$1(AttributionContext.scala:276)
# MAGIC at scala.util.DynamicVariable.withValue(DynamicVariable.scala:62)
# MAGIC at com.databricks.logging.AttributionContext$.withValue(AttributionContext.scala:272)
# MAGIC at com.databricks.logging.DLTAttributionContextHelper$.withAttributionContext(DLTAttributionContextHelper.scala:9)
# MAGIC at com.databricks.pipelines.execution.core.PhysicalFlow$$anon$1$$anon$2.run(PhysicalFlow.scala:305)
# MAGIC at com.databricks.pipelines.execution.core.CommandContextUtils$$anon$1.$anonfun$run$1(CommandContextUtils.scala:124)
# MAGIC at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
# MAGIC at com.databricks.pipelines.execution.core.CommandContextUtils$.withCommandContext(CommandContextUtils.scala:99)
# MAGIC at com.databricks.pipelines.execution.core.CommandContextUtils$$anon$1.run(CommandContextUtils.scala:124)
# MAGIC at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.$anonfun$run$1(SparkThreadLocalForwardingThreadPoolExecutor.scala:157)
# MAGIC at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
# MAGIC at com.databricks.spark.util.IdentityClaim$.withClaim(IdentityClaim.scala:48)
# MAGIC at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.$anonfun$runWithCaptured$4(SparkThreadLocalForwardingThreadPoolExecutor.scala:113)
# MAGIC at com.databricks.unity.UCSEphemeralState$Handle.runWith(UCSEphemeralState.scala:51)
# MAGIC at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:112)
# MAGIC at org.apache.spark.util.threads.SparkThreadLocalCapturingHelper.runWithCaptured$(SparkThreadLocalForwardingThreadPoolExecutor.scala:89)
# MAGIC at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.runWithCaptured(SparkThreadLocalForwardingThreadPoolExecutor.scala:154)
# MAGIC at org.apache.spark.util.threads.SparkThreadLocalCapturingRunnable.run(SparkThreadLocalForwardingThreadPoolExecutor.scala:157)
# MAGIC at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
# MAGIC at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
# MAGIC at java.lang.Thread.run(Thread.java:750)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fix the failure by changing from fail to drop in expectation
# MAGIC
# MAGIC Go to the `revenue_w_dq` notebook.
# MAGIC
# MAGIC Change
# MAGIC
# MAGIC ```
# MAGIC @dlt.expect_all_or_fail()
# MAGIC ```
# MAGIC
# MAGIC To 
# MAGIC
# MAGIC ```
# MAGIC @dlt.expect_all_or_drop()
# MAGIC ```
# MAGIC
# MAGIC Run pipeline again by pressing start. It should now work.
