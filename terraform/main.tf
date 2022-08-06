terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.77.0"
    }
  }
  required_version = ">= 0.13.0"
}

module "vpc" {
  source = "./modules/vpc"

  project_id  = var.project_id
  
}

module "gke" {
  source = "./modules/gke"

  project_id    = var.project_id
  cluster_name  = "airflow-gke-data-bootcamp-1"
  location      = var.location
  vpc_id        = module.vpc.vpc
  subnet_id     = module.vpc.private_subnets[0]
  gke_num_nodes = var.gke_num_nodes
  machine_type  = var.machine_type

}

module "cloudsql" {
  source = "./modules/cloudsql"

  project_id        = var.project_id
  region            = var.region
  location          = var.location
  instance_name     = var.instance_name
  database_version  = var.database_version
  instance_tier     = var.instance_tier
  disk_space        = var.disk_space
  database_name     = var.database_name
  db_username       = var.db_username
  db_password       = var.db_password
  data_1_path       = var.data_1_path
  data_2_path       = var.data_2_path
  data_3_path       = var.data_3_path
  data_1_name       = var.data_1_name
  data_2_name       = var.data_2_name
  data_3_name       = var.data_3_name
}

# Storage Bucket and Data
resource "google_storage_bucket" "input_bucket" {
  name     = "${var.project_id}-storage"
  location = var.region
}

resource "google_storage_bucket_object" "data_1" {
  name   = var.data_1_name
  source = var.data_1_path
  bucket = google_storage_bucket.input_bucket.name
}

resource "google_storage_bucket_object" "data_2" {
  name   = var.data_2_name
  source = var.data_2_path
  bucket = google_storage_bucket.input_bucket.name
}

resource "google_storage_bucket_object" "data_3" {
  name   = var.data_3_name
  source = var.data_3_path
  bucket = google_storage_bucket.input_bucket.name
}

resource "google_storage_bucket_object" "jars" {
  name   = var.scala_scripts_file
  source = var.scala_scripts_path
  bucket = google_storage_bucket.input_bucket.name
}