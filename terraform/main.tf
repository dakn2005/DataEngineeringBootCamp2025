terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.18.0"
    }
  }
}

provider "google" {
  # Configuration options
# in the terminal export google credentials with your path to the key
  project     = "marine-base-449315-s5"
  region      = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "marine-base-449315-s5-terra-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}