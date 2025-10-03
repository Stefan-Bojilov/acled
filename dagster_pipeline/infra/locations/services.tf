module "ecs_service" {
  source = "terraform-aws-modules/ecs/aws//modules/service"

  name        = "dagster-acled-code-location"
  cluster_arn = data.aws_ecs_cluster.dagster.arn

  cpu    = 1024
  memory = 4096

  # Container definition(s)
  container_definitions = {
    # Key must match for the module's internal logic
    dagster_acled = merge(
      {
        name      = "dagster_acled"
        cpu       = 512
        memory    = 1024
        essential = true
        image     = docker_registry_image.dagster_acled.name

        memory_reservation = 100

        cloudwatch_log_group_name = "/aws/ecs/dagster-acled-code-location"

        # Command to start Dagster code server
        command = split(" ", "dagster code-server start -h 0.0.0.0 -p 4000 -m dagster_acled.definitions")

        # Secrets from AWS Secrets Manager
        secrets = concat(
          # Your code location specific secrets
          [
            for secret_name in local.code_location_secrets : {
              name      = secret_name
              valueFrom = data.aws_secretsmanager_secret.acled_secrets[secret_name].arn
            }
          ],
          # Dagster Postgres connection secrets
          [
            for var in local.dagster_env_vars : {
              name      = var
              valueFrom = data.aws_secretsmanager_secret.postgres[var].arn
            }
          ]
        )

        environment = [
          {
            name  = "DAGSTER_CURRENT_IMAGE"
            value = docker_registry_image.dagster_acled.name
          }
        ]

        service = "dagster-acled-code-location"

        # Images used require access to write to root filesystem
        readonly_root_filesystem = false
      },
      {
        log_configuration = {
          options = {
            awslogs-region        = var.region
            awslogs-group         = "/aws/ecs/dagster-acled-code-location"
            awslogs-create-group  = "true"
            awslogs-stream-prefix = "ecs"
          }
        }
        port_mappings = [
          {
            name          = "dagster_acled"
            containerPort = 4000
            protocol      = "tcp"
          }
        ]
      }
    )
  }

  service_connect_configuration = {
    service = {
      client_alias = {
        port     = 4000
        dns_name = "dagster-acled-code-location"
      }
      port_name      = "dagster_acled"
      discovery_name = "dagster-acled-code-location"
    }
  }

  subnet_ids = data.aws_subnets.private.ids

  security_group_rules = {
    dagster_instance_4000 = {
      type        = "ingress"
      from_port   = 4000
      to_port     = 4000
      protocol    = "tcp"
      description = "Talk to dagster instance."
      cidr_blocks = local.private_subnets_cidr_blocks
    }
    postgres_5432 = {
      type                     = "ingress"
      from_port                = 5432
      to_port                  = 5432
      protocol                 = "tcp"
      description              = "Dagster db connection."
      source_security_group_id = data.aws_security_group.postgres.id
    }
    egress_all = {
      type        = "egress"
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }

  create_task_exec_iam_role = false
  task_exec_iam_role_arn    = data.aws_iam_role.task_exec.arn

  # Task role for runtime permissions
  tasks_iam_role_name            = "dagster-acled-task-role"
  tasks_iam_role_use_name_prefix = false
  tasks_iam_role_statements = [
    {
      effect    = "Allow"
      actions   = ["s3:ListBucket"]
      resources = ["arn:aws:s3:::${var.s3_bucket_name}"]
    },
    {
      effect = "Allow"
      actions = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ]
      resources = ["arn:aws:s3:::${var.s3_bucket_name}/*"]
    },
    {
      effect = "Allow"
      actions = [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ]
      resources = [
        for secret_name in local.code_location_secrets :
        "arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:${secret_name}-*"
      ]
    }
  ]
}