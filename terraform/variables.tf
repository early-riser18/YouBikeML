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

variable "prefect_api_key" {}