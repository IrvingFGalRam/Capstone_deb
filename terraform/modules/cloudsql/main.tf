# DB
resource "random_id" "db_name_suffix" {
  byte_length = 4
}

resource "google_sql_database_instance" "sql_instance" {
  name              = "${var.instance_name}-${random_id.db_name_suffix.hex}"
  database_version  = var.database_version
  region            = var.region

  settings {
    tier      = var.instance_tier
    disk_size = var.disk_space

    location_preference {
      zone = var.location
    }

    ip_configuration {
      authorized_networks {
        value           = "0.0.0.0/0"
        name            = "test-cluster"
      }
    }
  }

  deletion_protection = "false"
}

resource "google_sql_database" "database" {
  name     = var.database_name
  instance = google_sql_database_instance.sql_instance.name
//  instance = var.instance_name
}

resource "google_sql_user" "users" {
  name     = var.db_username
  instance = google_sql_database_instance.sql_instance.name
//  instance = var.instance_name
  host     = "*"
  password = var.db_password
}