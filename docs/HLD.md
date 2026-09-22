# High-Level Design — Big Data Processing (EMR + Apache Spark)

## 1. Domain context

The Amazon Customer Reviews dataset records real product reviews at massive scale: `marketplace`, `customer_id`, `review_id`, `product_id`, `product_title`, `product_category`, `star_rating`, `helpful_votes`, `total_votes`, `review_date`, review text. Business questions this pipeline should answer:

- How has average star rating trended month-over-month per product category?
- Which products rank in the top-N by review volume/rating within their category each quarter?
- Are there categories where review volume is growing but average rating is declining (an early "quality problem" signal)?

These are exactly the aggregation/ranking/trend questions a customer-analytics or product team asks — and at this row count (tens–hundreds of millions), they require distributed compute, not a single machine.

## 2. Architecture overview

```
 s3://amazon-reviews-pds/parquet/product_category=Electronics/*.parquet
 s3://amazon-reviews-pds/parquet/product_category=Books/*.parquet
                       │
                       ▼
        ┌───────────────────────────────┐
        │  Amazon EMR cluster             │
        │  ┌─────────┐ ┌────────┐ ┌─────┐ │
        │  │ Master   │ │ Core×2  │ │Task │ │
        │  │(m5.xlarge)│ │On-Demand│ │Spot │ │
        │  └─────────┘ └────────┘ └─────┘ │
        │        Spark on YARN             │
        └───────────────┬───────────────────┘
                         │ Step: spark-submit review_trends.py
                         ▼
        ┌───────────────────────────────┐
        │  S3 analytics/reviews/           │
        │  (rating trend + top-N tables,    │
        │   Parquet, partitioned)            │
        └───────────────┬───────────────────┘
                         │
                         ▼
              Glue Catalog → Athena
                         │
              cluster auto-terminates
              after the step completes
```

## 3. Components

| Component | Purpose |
|---|---|
| Public S3 dataset | Source data — read directly, no copy/ingest step needed (this project starts at "already in S3," letting it focus entirely on distributed compute) |
| EMR cluster | Managed Hadoop/Spark cluster; master coordinates, core nodes hold HDFS (used only for shuffle spill, not primary storage since data is read/written via EMRFS to S3), task nodes are pure additional compute |
| Spark job | PySpark: reads Parquet, aggregates, window functions, joins, writes Parquet output |
| S3 `analytics/reviews/` | Job output, partitioned by category/year/month |
| Glue Catalog / Athena | Downstream querying of the job's output, same pattern as Projects 1 and 2 |
| Auto-termination | Cluster is ephemeral — created, runs the one step, terminates; no idle cluster billing |

## 4. Data flow

1. `aws emr create-cluster` launches the cluster with the Spark step already attached (or steps added after cluster is `WAITING`), pointing at `spark_jobs/review_trends.py` staged in S3.
2. Spark job reads the public dataset directly from `s3://amazon-reviews-pds/parquet/product_category={Electronics,Books}/` via EMRFS.
3. Computes: (a) monthly avg rating + review count per category, (b) a window-function ranking of top-10 products by review volume per category per quarter, (c) a join between the two categories' top-product lists to compare category health side by side.
4. Writes results to `s3://<bucket>/analytics/reviews/`, partitioned by `category`, `year`.
5. Cluster configured with `--auto-terminate` (or `KeepJobFlowAliveWhenNoSteps=false`) so it shuts down automatically once the step finishes — no manual cleanup, no idle-cluster cost.
6. A crawler (or the job itself) registers the output in Glue Catalog; Athena queries it exactly as in Project 1.

## 5. Key exam concepts exercised

- EMR cluster architecture: master/core/task node roles and what runs where.
- Spot vs. On-Demand instance strategy for cost-optimized, fault-tolerant compute (task nodes are the "losable" tier).
- Storage levels in Spark (`MEMORY_ONLY` vs. `MEMORY_AND_DISK`) for cached/reused DataFrames.
- Shuffle partition tuning (`spark.sql.shuffle.partitions`) relative to cluster core count and data volume.
- Cost-saving cluster lifecycle patterns (auto-termination, Spot pricing for the losable compute tier).

## 6. Non-functional considerations

- **Cost:** this is the most expensive project per-run of the four (multi-node cluster, even briefly) — auto-termination and Spot task nodes are not optional extras here, they're the point of the exercise.
- **Fault tolerance:** losing a Spot task node mid-job costs Spark a re-computed stage at worst (YARN reschedules), not a job failure — core nodes stay On-Demand specifically so losing capacity never touches data actually needed for shuffle-in-progress correctness on the last surviving core.
- **Security:** EMR security configuration enforces S3 SSE-KMS at rest for both S3 and local disk. TLS-in-transit and a private-subnet-plus-VPC-endpoint layout are documented as the production-hardening path (LLD §5) rather than implemented in the reference Terraform, which deploys into the account's default VPC/subnet to keep the stack focused on the EMR/Spark concepts this project is meant to teach.

## 7. Infrastructure as Code (Terraform)

[`infra/`](../infra/) provisions the S3 bucket, the EMR security configuration, both IAM roles/instance profile, the cluster itself (master + core instance groups, plus a Spot task instance group added post-creation), and a Glue database/crawler for the output. This is the one project in the portfolio where Terraform manages something genuinely **ephemeral** rather than steady-state infrastructure — see LLD §6 for how `apply`/`destroy` interact with the cluster's own auto-termination.

```
infra/
├── main.tf                     # security config, IAM, cluster, task group, output crawler, alerting
├── variables.tf                  # review_categories, core/task counts, keep_cluster_alive, Spot price
├── outputs.tf
├── terraform.tfvars.example
└── modules/                        # vendored — kms_key, s3_data_lake_bucket, alerting
```

**Workflow:**

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set a globally-unique bucket_name
terraform init
terraform apply    # this both creates the cluster AND runs the Spark step; apply blocks until the cluster reaches WAITING/RUNNING
```

With the default `keep_cluster_alive = false`, the cluster runs the `ReviewTrends` step and **auto-terminates itself** — so shortly after `apply` finishes, the cluster is already gone even though it still exists as a Terraform-managed resource (in `TERMINATED` state). `terraform destroy` at that point is a fast no-op for the cluster itself but still cleans up the IAM roles, security configuration, and bucket. If you set `keep_cluster_alive = true` for interactive exploration, **you must run `terraform destroy` yourself** when done — nothing will stop the per-hour billing otherwise. This is the sharpest cost-safety edge in the whole portfolio; see LLD §6 for the full explanation.
