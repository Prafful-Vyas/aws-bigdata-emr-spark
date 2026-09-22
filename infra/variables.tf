variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short name used to prefix/tag all resources."
  type        = string
  default     = "reviews-trend"
}

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for the Spark script, EMR logs, and job output. Must be changed from the example before applying."
  type        = string
}

variable "review_categories" {
  description = "product_category values to read from the public s3://amazon-reviews-pds dataset. Keep this to 1-3 categories to bound cost/runtime."
  type        = list(string)
  default     = ["Electronics", "Books"]
}

variable "core_instance_count" {
  description = "Number of On-Demand core nodes (hold HDFS/shuffle data)."
  type        = number
  default     = 2
}

variable "task_instance_count" {
  description = "Number of Spot task nodes (pure compute, safely interruptible)."
  type        = number
  default     = 2
}

variable "task_spot_max_price" {
  description = "Max Spot price (USD/hr) for task nodes. Check current Spot pricing for the instance type/region before applying."
  type        = string
  default     = "0.20"
}

variable "keep_cluster_alive" {
  description = "false = classic 'run the step then auto-terminate' ephemeral cluster (matches the HLD/LLD). true = cluster stays up after the step for interactive use; remember to `terraform destroy` when done to stop billing."
  type        = bool
  default     = false
}

variable "alert_email" {
  description = "Optional email to subscribe to the pipeline alert SNS topic."
  type        = string
  default     = ""
}
