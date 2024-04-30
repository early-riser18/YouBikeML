variable "aws_region" {
  type    = string
  default = "ap-northeast-1"
}

variable "s3_bucket_stage_name" {
  type    = string
  default = "stage-youbike"
}

variable "org_name" {
  type    = string
  default = "early-riser18-perso"
}


variable "cockroachdb_sql_user_password" {}
variable "prefect_api_key" {}
variable "cockroachdb_api_key" {}