terraform {
  backend "s3" {
    key = "infra/dagster_acled/terraform.tfstate"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">=5.36.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "3.0.2"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project      = local.project_name
      CodeLocation = "dagster_acled"
    }
  }
}

provider "docker" {
  registry_auth {
    address  = data.aws_ecr_authorization_token.dagster.proxy_endpoint
    username = data.aws_ecr_authorization_token.dagster.user_name
    password = data.aws_ecr_authorization_token.dagster.password
  }
}

locals {
  project_name = "dagster-ecs-poc"
  cluster_name = "${local.project_name}-cluster"
  account_id   = data.aws_caller_identity.current.account_id
  
  private_subnets_cidr_blocks = [
    for id in data.aws_subnets.private.ids : data.aws_subnet.private[id].cidr_block
  ]
  
  # Dagster Postgres environment variables (same as blog post)
  dagster_env_vars = toset([
    "DAGSTER_POSTGRES_USER",
    "DAGSTER_POSTGRES_PASSWORD",
    "DAGSTER_POSTGRES_HOSTNAME",
    "DAGSTER_POSTGRES_DB"
  ])
}

data "aws_caller_identity" "current" {}

data "aws_ecr_authorization_token" "dagster" {
  registry_id = local.account_id
}

data "aws_ecs_cluster" "dagster" {
  cluster_name = local.cluster_name
}

data "aws_vpc" "dagster" {
  tags = {
    Project = local.project_name
  }
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.dagster.id]
  }

  tags = {
    Tier = "Private"
  }
}

data "aws_subnet" "private" {
  for_each = toset(data.aws_subnets.private.ids)
  id       = each.value
}

data "aws_iam_role" "task_exec" {
  name = "${local.project_name}-task-exec-role"
}

data "aws_security_group" "postgres" {
  vpc_id = data.aws_vpc.dagster.id
  name   = "${local.project_name}-postgres-sg"
}

# Reference to your ACLED-specific secrets
data "aws_secretsmanager_secret" "acled_secrets" {
  for_each = local.code_location_secrets
  name     = each.value
}

# Reference to Dagster Postgres secrets (shared with other code locations)
data "aws_secretsmanager_secret" "postgres" {
  for_each = local.dagster_env_vars
  name     = each.value
}