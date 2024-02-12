variable "credentials_file" {
  description = "Path to the credentials file"
  default     = "../../key/maverick-413820-d1061eb0cce1.json"
}

variable "project" {
  description = "Project"
  default     = "maverick-413820"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "nyc_taxi_24"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket name"
  default     = "maverick-nyc-taxi-24"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
