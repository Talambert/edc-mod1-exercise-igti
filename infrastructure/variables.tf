variable "base_bucket_name" {
  default = "datalake-igti-tf-tancredo-2021"
}

variable "ambiente" {
  default = "producao"
}

variable "numero_conta" {
  default = "421168935276"
}

variable "aws_region" {
  default = "us-east-2"
}


variable "lambda_function_name" {
  default = "IGTIexecutaEMR"
}

variable "key_pair_name" {
  default = "tancredo-igti-teste"
}

variable "airflow_subnet_id" {
  default = "subnet-4cef5427"
}

variable "vpc_id" {
  default = "vpc-d724b4bc"
}


