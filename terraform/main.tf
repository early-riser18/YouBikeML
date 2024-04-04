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

#AWS NETWORKING
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


resource "aws_iam_role" "prefect_execution_role_stage" {
  name = "prefect-execution-role-stage"
  assume_role_policy = jsonencode({
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      },
    ]
    Version = "2012-10-17"
  })
  managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"]
}

resource "aws_iam_role_policy" "logs_allow_create_log_group_stage" {
  name   = "logs-allow-create-log-group-stage"
  role   = aws_iam_role.prefect_execution_role_stage.id
  policy = jsonencode({
    Statement = [
      {
        Action   = ["logs:CreateLogGroup"]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
    Version = "2012-10-17"
  })
}

# IAM Role for Prefect Agent Task
resource "aws_iam_role" "prefect_task_role_stage" {
  name = "prefect-task-role-stage"
  assume_role_policy = jsonencode({
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      },
    ]
    Version = "2012-10-17"
  })
}

resource "aws_iam_role_policy" "prefect_allow_ecs_task_stage" {
  name   = "prefect-allow-ecs-task-stage"
  role   = aws_iam_role.prefect_task_role_stage.id
  policy = jsonencode({
    Statement = [
      {
        Action   = [
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
          "logs:PutLogEvents",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
    Version = "2012-10-17"
  })
}

# ECS Cluster for Prefect 
resource "aws_ecs_cluster" "prefect_cluster_stage" {
  name = "prefect-stage"
}

resource "aws_ecs_cluster_capacity_providers" "prefect_cluster_capacity_providers_stage" {
  cluster_name       = aws_ecs_cluster.prefect_cluster_stage.name
  capacity_providers = ["FARGATE"]
}

# CloudWatch Log Group for Prefect 
resource "aws_cloudwatch_log_group" "prefect_agent_log_group" {
  name              = "prefect-agent-log-group-stage"
  retention_in_days = 14
}

resource "aws_iam_user_policy_attachment" "ecs_task_definition_attach" {
  user       = "prefect-ecs"
  policy_arn = "arn:aws:iam::aws:policy/AmazonECS_FullAccess"
}

resource "aws_ecr_repository" "prefect-flows" {
  name                 = "prefect-flows"
  image_tag_mutability = "MUTABLE"

}

resource "aws_lambda_function" "get-youbike-forecast" {
  function_name = "get-youbike-forecast"
  role          = aws_iam_role.lambda-ml-model.arn
  image_uri     = "211125707335.dkr.ecr.ap-northeast-1.amazonaws.com/prefect-flows:latest" #Needs to refactor to variable
  image_config {
    command     = ["predict.forecast_service.lambda_handler"]
    entry_point = ["/usr/local/bin/python", "-m", "awslambdaric"]
  }
  memory_size = 500
  ephemeral_storage {
    size = 1000
  }
  timeout      = 60
  package_type = "Image"
  environment {
    variables = {
      "APP_ENV" : "stage",
      "PREFECT_HOME" : "/tmp"
    }
  }
}
resource "aws_iam_role" "lambda-ml-model" {
  name = "lambda-ml-model"
  assume_role_policy = jsonencode({
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
    Version = "2012-10-17"
  })
  managed_policy_arns = ["arn:aws:iam::aws:policy/AmazonS3FullAccess", "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"]
}

resource "aws_lambda_function_url" "test_latest" {
  function_name      = aws_lambda_function.get-youbike-forecast.function_name
  authorization_type = "NONE"
}