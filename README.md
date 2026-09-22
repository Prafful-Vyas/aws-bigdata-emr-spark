# Project 4 — Big Data Processing: Amazon EMR + Apache Spark

**Domain:** Customer / product analytics at scale (voice-of-customer, review sentiment trends)
**Dataset:** [Amazon Customer Reviews Dataset](https://s3.amazonaws.com/amazon-reviews-pds/readme.html) — hundreds of millions of real Amazon product reviews (1995–2015), publicly hosted in the `s3://amazon-reviews-pds/parquet/` bucket, partitioned by `product_category`. No download needed — it's already a public S3 bucket you can read from directly (requester pays: no; publicly readable). Pick 2–3 categories (e.g., `Electronics`, `Books`) to keep runtime/cost reasonable — those alone are multi-GB and tens of millions of rows, genuinely too large for a single-node job.

## Why this domain

Customer review/sentiment analytics at scale is used across every retail and SaaS company for product decisions, and this dataset is large enough to force real EMR/Spark tuning decisions (shuffle partitions, executor sizing, broadcast joins, Spot vs. On-Demand) rather than running trivially on a laptop — which is the whole point of the EMR project relative to Project 1's single-Glue-worker batch job.

## What this project builds

An EMR cluster (1 master, on-demand core nodes, Spot task nodes) running a PySpark job that reads the public reviews Parquet dataset directly from S3, computes category/monthly rating trends and top-product rankings via window functions, joins across categories, and writes results back to S3 (registered in Glue Catalog for Athena). The cluster auto-terminates after the step completes.

See [`docs/HLD.md`](docs/HLD.md) and [`docs/LLD.md`](docs/LLD.md) for cluster sizing, Spark tuning, instance group config, and auto-termination setup.

## Deploying with Terraform

⚠️ **This is the one project where `terraform apply` itself spins up a real, billed, multi-node cluster** — not just idle infrastructure. Read [`docs/LLD.md` §8](docs/LLD.md#8-terraform-resource-inventory) before running this one.

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set a globally-unique bucket_name
terraform init
terraform apply
```

With the default `keep_cluster_alive = false`, the cluster runs the Spark step and **auto-terminates itself** shortly after `apply` finishes — check status with:

```bash
aws emr describe-cluster --cluster-id "$(terraform -chdir=infra output -raw cluster_id)" --query "Cluster.Status"
```

Query the output afterward via Athena (table registered by the Glue crawler Terraform created) or set `keep_cluster_alive = true` in `terraform.tfvars` if you want to SSH in / use the Spark UI interactively — if you do, **you are responsible for tearing it down**:

```bash
cd infra
terraform destroy
```

## Folder layout

```
04-bigdata-emr-spark/                # this folder is its own git repo
├── .gitignore
├── README.md
├── docs/
│   ├── HLD.md
│   └── LLD.md
├── spark_jobs/
│   └── review_trends.py
└── infra/                          # Terraform: security config, IAM, EMR cluster + task group, output crawler
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    ├── terraform.tfvars.example
    └── modules/                      # vendored — kms_key, s3_data_lake_bucket, alerting
```
