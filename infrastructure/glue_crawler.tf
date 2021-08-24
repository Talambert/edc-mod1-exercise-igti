resource "aws_glue_catalog_database" "censo" {
    name = "db_censo"
}

resource "aws_glue_crawler" "censo" {
  database_name = aws_glue_catalog_database.censo.name
  name          = "censo_s3_crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.datalake.id}/staging-zone/enem"
  }
}