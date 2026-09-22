output "bucket_name" {
  value = module.data_lake.bucket_name
}

output "cluster_id" {
  value = aws_emr_cluster.reviews_trend.id
}

output "cluster_name" {
  value = aws_emr_cluster.reviews_trend.name
}

output "analytics_database" {
  value = aws_glue_catalog_database.reviews_analytics.name
}

output "sns_topic_arn" {
  value = module.alerts.topic_arn
}
