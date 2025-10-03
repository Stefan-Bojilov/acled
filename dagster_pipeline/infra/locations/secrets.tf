

locals {
  code_location_secrets = toset([
    "acled_bucket",      # S3 bucket credentials
    "acled_postgres",    # PostgreSQL credentials
    "ACLED-API"          # ACLED API key
  ])
}

# Create IAM policy to allow reading these specific secrets
module "dagster_acled_secrets_policy" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-policy"
  version = "~> 5.0"

  name        = "dagster-acled-secrets-access"
  description = "Allow dagster_acled code location to read required secrets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          for secret_name in local.code_location_secrets :
          "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:${secret_name}-*"
        ]
      }
    ]
  })
}

# Attach policy to the task execution role
resource "aws_iam_role_policy_attachment" "dagster_acled_secrets" {
  role       = data.aws_iam_role.task_exec.name
  policy_arn = module.dagster_acled_secrets_policy.arn
}