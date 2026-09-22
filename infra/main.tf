module "kms" {
  source      = "./modules/kms_key"
  name        = "${var.project_name}-key"
  description = "CMK for the EMR/Spark customer reviews analytics pipeline."
}

module "data_lake" {
  source      = "./modules/s3_data_lake_bucket"
  bucket_name = var.bucket_name
  kms_key_arn = module.kms.key_arn
}

module "alerts" {
  source      = "./modules/alerting"
  name        = "${var.project_name}-alerts"
  kms_key_arn = module.kms.key_arn
  alert_email = var.alert_email
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_s3_object" "spark_job" {
  bucket = module.data_lake.bucket_name
  key    = "spark_jobs/review_trends.py"
  source = "${path.module}/../spark_jobs/review_trends.py"
  etag   = filemd5("${path.module}/../spark_jobs/review_trends.py")
}

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

resource "aws_iam_role" "emr_service" {
  name = "${var.project_name}-emr-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_managed" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2" {
  name = "${var.project_name}-emr-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "emr_ec2_scoped" {
  name = "${var.project_name}-emr-ec2-scoped-policy"
  role = aws_iam_role.emr_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadPublicReviewsDataset"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = ["arn:aws:s3:::amazon-reviews-pds", "arn:aws:s3:::amazon-reviews-pds/*"]
      },
      {
        Sid      = "ProjectBucketReadWrite"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = [module.data_lake.bucket_arn, "${module.data_lake.bucket_arn}/*"]
      },
      {
        Sid    = "GlueCatalogAsMetastore"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
          "glue:GetPartition", "glue:GetPartitions", "glue:CreateTable", "glue:UpdateTable",
          "glue:BatchCreatePartition"
        ]
        Resource = ["*"]
      },
      {
        Sid      = "KmsForData"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [module.kms.key_arn]
      },
      {
        Sid      = "Logging"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.project_name}-emr-ec2-instance-profile"
  role = aws_iam_role.emr_ec2.name
}

# ---------------------------------------------------------------------------
# Encryption
# ---------------------------------------------------------------------------

resource "aws_emr_security_configuration" "this" {
  name = "${var.project_name}-security-config"

  configuration = jsonencode({
    EncryptionConfiguration = {
      EnableInTransitEncryption = false # would require a custom TLS certificate provider bootstrap - out of scope for this personal project
      EnableAtRestEncryption    = true
      AtRestEncryptionConfiguration = {
        S3EncryptionConfiguration = {
          EncryptionMode = "SSE-KMS"
          AwsKmsKey      = module.kms.key_arn
        }
        LocalDiskEncryptionConfiguration = {
          EncryptionKeyProviderType = "AwsKms"
          AwsKmsKey                 = module.kms.key_arn
        }
      }
    }
  })
}

# ---------------------------------------------------------------------------
# EMR cluster: 1 On-Demand master, N On-Demand core (HDFS/shuffle), and a
# separate Spot task instance group for pure additional compute.
# ---------------------------------------------------------------------------

resource "aws_emr_cluster" "reviews_trend" {
  name                              = "${var.project_name}-cluster"
  release_label                     = "emr-7.1.0"
  applications                      = ["Spark"]
  service_role                      = aws_iam_role.emr_service.arn
  log_uri                           = "s3://${module.data_lake.bucket_name}/emr-logs/"
  security_configuration            = aws_emr_security_configuration.this.name
  keep_job_flow_alive_when_no_steps = var.keep_cluster_alive
  termination_protection            = false

  dynamic "auto_termination_policy" {
    for_each = var.keep_cluster_alive ? [1] : []
    content {
      idle_timeout = 3600
    }
  }

  ec2_attributes {
    subnet_id        = data.aws_subnets.default.ids[0]
    instance_profile  = aws_iam_instance_profile.emr_ec2.arn
  }

  master_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 1
  }

  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = var.core_instance_count
  }

  step {
    name              = "ReviewTrends"
    action_on_failure = "TERMINATE_CLUSTER"

    hadoop_jar_step {
      jar  = "command-runner.jar"
      args = concat(
        ["spark-submit", "--deploy-mode", "cluster",
         "s3://${module.data_lake.bucket_name}/${aws_s3_object.spark_job.key}",
         module.data_lake.bucket_name],
        var.review_categories
      )
    }
  }

  configurations_json = jsonencode([
    {
      Classification = "hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    },
    {
      Classification = "spark-hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ])

  tags = {
    Name = "${var.project_name}-cluster"
  }

  depends_on = [aws_s3_object.spark_job]
}

# Task instance group added post-creation, Spot-priced - the "losable" tier.
# Only meaningful when the cluster stays up long enough to need extra capacity;
# harmless (just idle) in the default one-step-and-terminate mode.
resource "aws_emr_instance_group" "task" {
  cluster_id     = aws_emr_cluster.reviews_trend.id
  name           = "task-spot"
  instance_type  = "m5.xlarge"
  instance_count = var.task_instance_count
  bid_price      = var.task_spot_max_price
}

# ---------------------------------------------------------------------------
# Downstream Athena access to the job's output
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "reviews_analytics" {
  name = "reviews_analytics"
}

resource "aws_iam_role" "glue_crawler" {
  name = "${var.project_name}-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_crawler" {
  name = "${var.project_name}-glue-crawler-policy"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [module.data_lake.bucket_arn, "${module.data_lake.bucket_arn}/analytics/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = [module.kms.key_arn]
      }
    ]
  })
}

resource "aws_glue_crawler" "reviews_analytics" {
  name          = "${var.project_name}-analytics-crawler"
  database_name = aws_glue_catalog_database.reviews_analytics.name
  role          = aws_iam_role.glue_crawler.arn

  s3_target {
    path = "s3://${module.data_lake.bucket_name}/analytics/reviews/"
  }
}

# ---------------------------------------------------------------------------
# Monitoring: cluster terminated-with-errors -> SNS
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "cluster_failed" {
  name = "${var.project_name}-cluster-failed"

  event_pattern = jsonencode({
    source        = ["aws.emr"]
    "detail-type" = ["EMR Cluster State Change"]
    detail = {
      state = ["TERMINATED_WITH_ERRORS"]
      name  = [aws_emr_cluster.reviews_trend.name]
    }
  })
}

resource "aws_cloudwatch_event_target" "cluster_failed_sns" {
  rule = aws_cloudwatch_event_rule.cluster_failed.name
  arn  = module.alerts.topic_arn
}

data "aws_iam_policy_document" "sns_eventbridge" {
  statement {
    sid    = "AllowEventBridgePublish"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    actions   = ["SNS:Publish"]
    resources = [module.alerts.topic_arn]
  }
}

resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn    = module.alerts.topic_arn
  policy = data.aws_iam_policy_document.sns_eventbridge.json
}
