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

variable "ecr_image_uri" {
  type= string
  default = "211125707335.dkr.ecr.ap-northeast-1.amazonaws.com/prefect-flows:latest"
}

variable "cockroachdb_sql_user_password" {}
variable "prefect_api_key" {}
variable "cockroachdb_api_key" {}