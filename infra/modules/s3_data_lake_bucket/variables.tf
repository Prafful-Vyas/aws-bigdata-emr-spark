variable "bucket_name" {
  description = "Globally-unique S3 bucket name."
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the customer-managed KMS key used for default bucket encryption."
  type        = string
}

variable "force_destroy" {
  description = "Allow `terraform destroy` to delete this bucket even if it still contains objects. Personal-project default is true for easy teardown; set false to protect data you actually want to keep."
  type        = bool
  default     = true
}

variable "lifecycle_rules" {
  description = "Optional list of lifecycle transition rules, e.g. [{ id = \"raw-to-glacier-ir\", prefix = \"raw/\", transition_days = 90, storage_class = \"GLACIER_IR\" }]"
  type = list(object({
    id              = string
    prefix          = string
    transition_days = number
    storage_class   = string
  }))
  default = []
}

variable "tags" {
  description = "Tags applied to the bucket."
  type        = map(string)
  default     = {}
}
