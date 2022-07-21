output "instance_connection_name" {
//  value = "${var.project_id}:${var.region}:${var.instance_name}"
  value = google_sql_database_instance.sql_instance.connection_name
}

output "instance_ip_address" {
//  value = "Found at tfstate"
  value = google_sql_database_instance.sql_instance.ip_address
}

output "database_connection" {
  value = google_sql_database.database.self_link
}

output "database" {
  value = google_sql_database.database.id
}