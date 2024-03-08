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
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }
  required_version = ">=1.2.0"

}

provider "aws" {
  region  = var.aws_region
  profile = "personal"
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
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/24"
  instance_tenancy     = "default"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = {
    Name = "vpc-stage "
  }
}
resource "aws_subnet" "prefect_ecs_public_subnet" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.0.0.0/24"
  tags = {
    Name = "prefect_ecs_public_subnet"
  }
}

resource "aws_route_table_association" "prefect_ecs_subnet" {
  subnet_id      = aws_subnet.prefect_ecs_public_subnet.id
  route_table_id = aws_route_table.public-route-table.id
}
resource "aws_internet_gateway" "default" {
  vpc_id = aws_vpc.main.id
  tags = {
    Name = "default"
  }
}
resource "aws_route_table" "public-route-table" {
  vpc_id = aws_vpc.main.id
  tags = {
    Name = "public-route-table"
  }
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id  = aws_internet_gateway.default.id
  }
}


module "prefect_ecs_agent" {
  source = "github.com/PrefectHQ/prefect-recipes//devops/infrastructure-as-code/aws/tf-prefect2-ecs-agent"

  name                        = "stage"
  prefect_account_id          = "89a0da5e-d24f-4c42-9c16-ba705499f108"
  prefect_workspace_id        = "d36bb0e9-91d2-468d-a0e2-9350c7ee85fc"
  agent_subnets               = [aws_subnet.prefect_ecs_public_subnet.id]
  agent_queue_name            = "ecs-agent"
  vpc_id                      = aws_vpc.main.id
  agent_log_retention_in_days = 14
  agent_memory                = 512
  agent_cpu                   = 256
  prefect_api_key             = "pnu_B3WA26bV1aWk6dFgdDXpnumHKEXv8N456mQ5"


}

resource "aws_iam_policy" "patched_prefect_task_role" {
  description = "An extra policy to fix improper default configuration of module prefect_ecs_agent"
  name        = "patched_prefect_module_task_role"
  policy      = <<EOF
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Action": [
				"ec2:DescribeSubnets",
				"ec2:DescribeVpcs",
				"ecr:BatchCheckLayerAvailability",
				"ecr:BatchGetImage",
				"ecr:GetAuthorizationToken",
				"ecr:GetDownloadUrlForLayer",
				"ecs:DeregisterTaskDefinition",
				"ecs:DescribeTasks",
				"ecs:RegisterTaskDefinition",
				"ecs:RunTask",
				"ecs:StopTask",
				"ecs:TagResource",
				"iam:PassRole",
				"logs:CreateLogGroup",
				"logs:CreateLogStream",
				"logs:GetLogEvents",
				"logs:PutLogEvents"
			],
			"Effect": "Allow",
			"Resource": "*"
		}
	]
}
  EOF
}

resource "aws_iam_role_policy_attachment" "set_patched_prefect_task_role_policy" {
  role       = "prefect-agent-task-role-stage" #Hard coded because unable to access module.prefect_ecs_agent attributes programatically
  policy_arn = aws_iam_policy.patched_prefect_task_role.arn
}
