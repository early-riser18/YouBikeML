#for some ref https://www.clickittech.com/devops/terraform-best-practices/

terraform {
	backend "remote" {
	  organization = "early-riser18-perso"	

	workspaces {
	  name = "youbike"
	}
	}

  required_providers {
	aws = {
		source = "hashicorp/aws"
		version = "~> 4.16"
	}
  }
  required_version = ">=1.2.0"

}

provider "aws" {
	region = var.aws_region
}


resource "aws_s3_bucket" "s3_bucket_stage" {
	bucket = var.s3_bucket_stage_name
}

resource "aws_s3_bucket_public_access_block" "public_access_stage" {
  bucket = var.s3_bucket_stage_name

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}