# Low-Level Design — Big Data Processing (EMR + Apache Spark)

## 1. Source data

```
s3://amazon-reviews-pds/parquet/product_category=Electronics/*.parquet
s3://amazon-reviews-pds/parquet/product_category=Books/*.parquet
```

Public bucket, already partitioned by `product_category`, already Parquet — read directly with `spark.read.parquet(path)`; no crawler needed on the source side (it's someone else's bucket). Columns of interest: `marketplace`, `customer_id`, `review_id`, `product_id`, `product_title`, `star_rating (int)`, `helpful_votes (int)`, `total_votes (int)`, `review_date (date)`.

## 2. EMR cluster configuration

**Release label:** `emr-7.1.0` (Spark 3.5)
**Applications:** Spark

| Group | Role | Count | Instance type | Pricing |
|---|---|---|---|---|
| Master | coordinates YARN/Spark driver-adjacent processes | 1 | `m5.xlarge` | On-Demand (single point of failure — never Spot) |
| Core | HDFS (shuffle spill) + compute | `var.core_instance_count` (default 2) | `m5.xlarge` | On-Demand (holds shuffle data; losing a core node mid-shuffle is more disruptive than losing a task node) |
| Task | pure additional compute, no HDFS role | `var.task_instance_count` (default 2) | `m5.xlarge` | Spot, `bid_price = var.task_spot_max_price` |

**Instance fleets vs. instance groups:** the textbook-ideal version of this design (and the one worth knowing for the exam) uses **instance fleets** — `master_instance_fleet`/`core_instance_fleet` blocks plus a separately-added task fleet — specifically to get Spot **allocation strategy** (`capacity-optimized`, which picks the deepest Spot pools across several diversified instance types to minimize interruption rate) and managed scaling that reacts to YARN's `ContainerPendingRatio`. The Terraform in `infra/main.tf` instead uses the simpler **instance group** model (`master_instance_group`/`core_instance_group` + a separate `aws_emr_instance_group` for the task tier with a flat `bid_price`) — functionally equivalent for "On-Demand core, Spot task," but without allocation-strategy diversification or autoscaling. This trade favors a Terraform stack that reliably applies over one that demonstrates every fleet-specific knob; swapping in fleets is a documented enhancement, not a correction.

**Auto-termination:** with `keep_cluster_alive = false` (the default), `keep_job_flow_alive_when_no_steps = false` and the `ReviewTrends` step's `action_on_failure = "TERMINATE_CLUSTER"` together give the same effect as the CLI's `--auto-terminate` flag — the cluster runs the one step and shuts itself down, whether the step succeeds or fails, so a failed step never leaves a billed idle cluster running.

## 3. Spark job design (`spark_jobs/review_trends.py`)

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("review_trends")
         .config("spark.sql.shuffle.partitions", "96")   # see tuning note below
         .config("spark.sql.adaptive.enabled", "true")    # AQE coalesces shuffle partitions at runtime
         .getOrCreate())

categories = ["Electronics", "Books"]
df = (spark.read.parquet(*[f"s3://amazon-reviews-pds/parquet/product_category={c}/" for c in categories])
      .select("product_category", "product_id", "product_title", "star_rating", "review_date")
      .withColumn("year", F.year("review_date"))
      .withColumn("month", F.month("review_date")))

# 1. Monthly rating trend per category
trend = (df.groupBy("product_category", "year", "month")
          .agg(F.avg("star_rating").alias("avg_rating"), F.count("*").alias("review_count")))

# 2. Top-10 products by review volume per category per year, via window function
w = Window.partitionBy("product_category", "year").orderBy(F.desc("review_count"))
product_counts = (df.groupBy("product_category", "product_id", "product_title", "year")
                   .agg(F.count("*").alias("review_count"), F.avg("star_rating").alias("avg_rating")))
top_products = (product_counts.withColumn("rank", F.rank().over(w))
                .filter(F.col("rank") <= 10))

trend.repartition(4, "product_category").write.mode("overwrite") \
     .partitionBy("product_category", "year").parquet("s3://<bucket>/analytics/reviews/monthly_trend/")

top_products.repartition(4, "product_category").write.mode("overwrite") \
     .partitionBy("product_category", "year").parquet("s3://<bucket>/analytics/reviews/top_products/")
```

### Tuning notes

- **`spark.sql.shuffle.partitions = 96`**: rule of thumb 2–3× total executor cores. With 2 core + up to 6 task `m5.xlarge` (4 vCPU each) ≈ 32 cores at max scale → 64–96 shuffle partitions is a reasonable starting point; **Adaptive Query Execution (AQE)** is enabled specifically so Spark coalesces small post-shuffle partitions at runtime rather than requiring this number to be hand-tuned perfectly — the config sets the *ceiling*, AQE handles the rest. This ceiling-vs-AQE trade-off is worth stating explicitly since the exam tests the manual-tuning concept even though AQE reduces how often it's needed in practice.
- **Broadcast join threshold**: not explicitly needed in this job (both categories' data is large), but noted as the relevant lever (`spark.sql.autoBroadcastJoinThreshold`) if a small lookup dimension (e.g., a category-metadata table) were joined in.
- **Storage level**: `product_counts` is reused for both the trend and top-products computation paths in a fuller version of this job — cache it with `.persist(StorageLevel.MEMORY_AND_DISK)` rather than `MEMORY_ONLY`, since Spot task node loss would otherwise force a full shuffle recompute; `MEMORY_AND_DISK` spills to local disk instead of dropping the partition, trading some recompute-on-spill cost for resilience to Spot interruption specifically.
- **Small-file avoidance**: output `repartition(4, ...)` bounds the number of output files per partition, avoiding a small-files problem symmetrical to the one addressed in Project 1.

## 4. IAM

- `<project_name>-emr-service-role` — trust policy for `elasticmapreduce.amazonaws.com`, with the AWS-managed `AmazonElasticMapReduceRole` policy attached (standard for EMR to manage EC2/networking on your behalf).
- `<project_name>-emr-ec2-role` / `<project_name>-emr-ec2-instance-profile` — custom inline policy, **not** the AWS-managed `EMR_EC2_DefaultRole` default: `s3:GetObject`/`s3:ListBucket` on `arn:aws:s3:::amazon-reviews-pds` (source, public bucket — read-only) and on the project bucket (`spark_jobs/`, `analytics/`, `emr-logs/`); `glue:Get*`/`Create*`/`Update*`/`BatchCreatePartition` for using the Glue Data Catalog as the Hive metastore (`hive.metastore.client.factory.class` = `AWSGlueDataCatalogHiveClientFactory`, set via `configurations_json` on the cluster); `kms:Decrypt`/`GenerateDataKey` for the project's key.
- No permissions granted beyond this bucket/prefix set — explicitly not `s3:*`.

## 5. Security configuration

- EMR **Security Configuration** (`aws_emr_security_configuration.this`) attached to the cluster: at-rest encryption enabled for both S3 (SSE-KMS) and local/EBS disk (AwsKms provider), using the project's customer-managed key.
- **In-transit encryption (TLS between nodes) is deliberately left off** (`EnableInTransitEncryption = false`) — enabling it requires standing up a custom certificate provider (a script EMR calls at bootstrap to produce/distribute a PEM cert), which is real additional infrastructure orthogonal to what this project teaches. Documented here as the production-hardening step, not implemented.
- The cluster launches into the account's **default VPC**, in whichever subnet `data.aws_subnets.default.ids[0]` resolves to — not a private subnet with a Gateway VPC Endpoint as a hardened design would use (same trade-off Project 2 makes for Redshift Serverless, for the same reason: VPC design is a Solutions Architect exam topic, not the DEA-C01 focus this portfolio is built around).

## 6. Monitoring

- CloudWatch EMR metrics: `IsIdle` (confirms auto-termination is actually firing and the cluster isn't lingering billed-idle), `YARNMemoryAvailablePercentage` (catches under-provisioned task fleet), `AppsFailed`.
- Spark History Server / Spark UI (via SSH tunnel or the persistent History Server pointed at `s3://<bucket>/emr-logs/`) for post-hoc stage/shuffle inspection.
- CloudTrail captures `elasticmapreduce:RunJobFlow`/`AddJobFlowSteps` calls for audit.

## 7. Cost estimate

- On-demand pieces: 1× `m5.xlarge` master + 2× `m5.xlarge` core for the job's runtime (say 20–40 min for a 2-category run) ≈ well under $1 per run.
- Task fleet: Spot `m5.xlarge` typically 60–70% cheaper than On-Demand; scaling to 6 task nodes only while YARN has pending containers keeps this bounded.
- S3 storage for output: negligible (aggregated tables, not raw data, are what's persisted).
- The dominant cost lever for this project is **run duration × cluster size** — auto-termination is what keeps a forgotten cluster from becoming the most expensive line item in the whole portfolio.

## 8. Terraform resource inventory

| Resource | Terraform address | Notes |
|---|---|---|
| CMK, bucket, alert topic | `module.kms`, `module.data_lake`, `module.alerts` | vendored under `infra/modules/`, identical to the other three projects' copies |
| Security configuration | `aws_emr_security_configuration.this` | at-rest only, per §5 |
| IAM roles | `aws_iam_role.emr_service` / `.emr_ec2` + `aws_iam_instance_profile.emr_ec2` | per §4 |
| Spark script upload | `aws_s3_object.spark_job` | uploads `spark_jobs/review_trends.py`; MD5-tracked |
| Cluster | `aws_emr_cluster.reviews_trend` | master/core instance groups, the `ReviewTrends` step, `configurations_json` pointing the Hive metastore at Glue |
| Task group | `aws_emr_instance_group.task` | added via a separate resource because task groups attach *after* cluster creation; Spot via `bid_price` |
| Output crawler | `aws_glue_catalog_database.reviews_analytics` + `aws_glue_crawler.reviews_analytics` | makes `analytics/reviews/` queryable from Athena, same pattern as Project 1 |
| Alerting | `aws_cloudwatch_event_rule.cluster_failed` (`aws.emr` / `TERMINATED_WITH_ERRORS`) → SNS | |

**The `terraform apply`/`destroy` lifecycle here is genuinely different from the other three projects:**

- `apply` doesn't just create infrastructure that then sits idle waiting for you to use it — it **launches a real multi-node cluster and runs a Spark job**, and (with the default `keep_job_flow_alive_when_no_steps = false`) the cluster auto-terminates itself immediately after. Terraform's own `aws_emr_cluster` create call blocks until the cluster reaches a startable state, but the step execution and auto-termination happen asynchronously afterward — check cluster state with `aws emr describe-cluster` or the console, not just "did `terraform apply` succeed."
- Because the cluster terminates itself outside of a `terraform destroy` call, Terraform's state will show the cluster resource as existing (in `TERMINATED` state) even though nothing is being billed for it — this is expected, not drift to fight.
- If you flip `keep_cluster_alive = true` (e.g., to poke around interactively via SSH/Spark UI), the cluster stays up and **billing continues** until you either terminate it out-of-band or run `terraform destroy` — there is no automatic safety net once you've opted out of auto-termination. Treat this variable as "I am taking on manual responsibility for shutting this down."
- Re-running `terraform apply` to re-run the job (e.g., after editing `review_trends.py`) replaces the whole cluster (EMR clusters aren't mutable in the way a Glue job definition is) — expect a full new cluster spin-up each time, not an in-place update.
