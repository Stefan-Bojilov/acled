variable "region" {
  type        = string
  description = "The AWS region to deploy resources."
  default     = "eu-north-1"
}

variable "image_tag" {
  type        = string
  description = "Docker image tag for dagster_acled code location"
  default     = "v0.0.1"
}

variable "s3_bucket_name" {
  type        = string
  description = "S3 bucket name for ACLED data storage"
  default     = "dagster-acled-bucket"
}