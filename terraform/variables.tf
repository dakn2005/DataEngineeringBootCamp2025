
variable "location" {
    description = "Proj location"  
    default = "US"
}

variable "bq_dataset_name" {
    description = "BQ dataset name"  
    default = "de_dataset"
}

variable "gcs_storage_class" {
    description = "Bucket storage class"
    default = "STANDARD"

}

variable "gcs_bucket_name" {
    description = "My bucket name"
    default = "marine-base-449315-s5-terra-bucket"

}