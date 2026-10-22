locals {
  # awsworld names one-time schedules go-workflow-<message ID>.
  schedule_arn = var.enable_scheduler ? "${replace(aws_scheduler_schedule_group.workflows[0].arn, ":schedule-group/", ":schedule/")}/go-workflow-*" : null
}

resource "aws_dynamodb_table" "workflows" {
  name                        = var.name
  billing_mode                = "PAY_PER_REQUEST"
  hash_key                    = "pk"
  range_key                   = "sk"
  deletion_protection_enabled = var.deletion_protection

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
  attribute {
    name = "list_pk"
    type = "S"
  }
  attribute {
    name = "list_sk"
    type = "S"
  }
  attribute {
    name = "list_group_pk"
    type = "S"
  }
  attribute {
    name = "list_group_sk"
    type = "S"
  }

  global_secondary_index {
    name               = "list"
    projection_type    = "INCLUDE"
    non_key_attributes = ["status"]

    key_schema {
      attribute_name = "list_pk"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "list_sk"
      key_type       = "RANGE"
    }
  }

  global_secondary_index {
    name               = "list_group"
    projection_type    = "INCLUDE"
    non_key_attributes = ["status"]

    key_schema {
      attribute_name = "list_group_pk"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "list_group_sk"
      key_type       = "RANGE"
    }
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery
  }

  server_side_encryption {
    enabled = true
  }

  tags = var.tags
}

resource "aws_sqs_queue" "dead_letter" {
  name                      = "${var.name}-tasks-dlq"
  message_retention_seconds = var.message_retention_seconds
  sqs_managed_sse_enabled   = true
  tags                      = var.tags
}

resource "aws_sqs_queue" "tasks" {
  name                       = "${var.name}-tasks"
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.message_retention_seconds
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = var.max_receive_count
  })
  tags = var.tags
}

resource "aws_sqs_queue_redrive_allow_policy" "dead_letter" {
  queue_url = aws_sqs_queue.dead_letter.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.tasks.arn]
  })
}

resource "aws_lambda_event_source_mapping" "worker" {
  count = var.worker_function_name == null ? 0 : 1

  event_source_arn        = aws_sqs_queue.tasks.arn
  function_name           = var.worker_function_name
  batch_size              = var.batch_size
  function_response_types = ["ReportBatchItemFailures"]

  # Lambda checks the function's SQS permissions when the mapping is created.
  depends_on = [aws_iam_role_policy_attachment.worker]

  dynamic "scaling_config" {
    for_each = var.maximum_concurrency == null ? [] : [var.maximum_concurrency]
    content {
      maximum_concurrency = scaling_config.value
    }
  }
}

resource "aws_scheduler_schedule_group" "workflows" {
  count = var.enable_scheduler ? 1 : 0

  name = var.name
  tags = var.tags
}

resource "aws_iam_role" "scheduler" {
  count = var.enable_scheduler ? 1 : 0

  name = "${var.name}-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { ArnEquals = { "aws:SourceArn" = aws_scheduler_schedule_group.workflows[0].arn } }
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy" "scheduler" {
  count = var.enable_scheduler ? 1 : 0

  name = "send-to-tasks-queue"
  role = aws_iam_role.scheduler[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "sqs:SendMessage"
      Resource = aws_sqs_queue.tasks.arn
    }]
  })
}

data "aws_iam_policy_document" "worker" {
  statement {
    sid       = "Table"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:BatchGetItem"]
    resources = [aws_dynamodb_table.workflows.arn, "${aws_dynamodb_table.workflows.arn}/index/*"]
  }

  statement {
    sid       = "Queue"
    actions   = ["sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.tasks.arn]
  }

  dynamic "statement" {
    for_each = var.enable_scheduler ? [1] : []
    content {
      sid       = "Schedules"
      actions   = ["scheduler:CreateSchedule", "scheduler:DeleteSchedule"]
      resources = [local.schedule_arn]
    }
  }

  dynamic "statement" {
    for_each = var.enable_scheduler ? [1] : []
    content {
      sid       = "PassSchedulerRole"
      actions   = ["iam:PassRole"]
      resources = [aws_iam_role.scheduler[0].arn]
      condition {
        test     = "StringEquals"
        variable = "iam:PassedToService"
        values   = ["scheduler.amazonaws.com"]
      }
    }
  }
}

data "aws_iam_policy_document" "api" {
  statement {
    sid       = "Table"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:BatchGetItem"]
    resources = [aws_dynamodb_table.workflows.arn, "${aws_dynamodb_table.workflows.arn}/index/*"]
  }

  statement {
    sid       = "Queue"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.tasks.arn]
  }

  dynamic "statement" {
    for_each = var.enable_scheduler ? [1] : []
    content {
      sid       = "CancelSchedules"
      actions   = ["scheduler:DeleteSchedule"]
      resources = [local.schedule_arn]
    }
  }
}

resource "aws_iam_policy" "worker" {
  name        = "${var.name}-worker"
  description = "Access for the awsworld.Queue worker."
  policy      = data.aws_iam_policy_document.worker.json
  tags        = var.tags
}

resource "aws_iam_policy" "api" {
  name        = "${var.name}-api"
  description = "Access for the awsworld.HTTP app-facing API."
  policy      = data.aws_iam_policy_document.api.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "worker" {
  count = var.worker_role_name == null ? 0 : 1

  role       = var.worker_role_name
  policy_arn = aws_iam_policy.worker.arn
}

resource "aws_iam_role_policy_attachment" "api" {
  count = var.api_role_name == null ? 0 : 1

  role       = var.api_role_name
  policy_arn = aws_iam_policy.api.arn
}
