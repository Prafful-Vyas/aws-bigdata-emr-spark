variable "name" {
  description = "Base name used for the KMS key alias (alias/<name>)."
  type        = string
}

variable "description" {
  description = "Description attached to the KMS key."
  type        = string
  default     = "Customer-managed key for AWS data engineering project"
}

variable "deletion_window_in_days" {
  description = "Waiting period before the key is deleted after being scheduled for deletion."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags applied to the key."
  type        = map(string)
  default     = {}
}
