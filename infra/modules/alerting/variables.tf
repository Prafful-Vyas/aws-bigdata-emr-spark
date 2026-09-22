variable "name" {
  description = "Name of the SNS topic."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key used to encrypt the topic at rest."
  type        = string
}

variable "alert_email" {
  description = "Optional email address to subscribe for alerts. Leave blank to skip the subscription and add one manually later."
  type        = string
  default     = ""
}

variable "tags" {
  type    = map(string)
  default = {}
}
