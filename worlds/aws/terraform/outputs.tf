output "table_name" {
  description = "DynamoDB table name, for awsworld.Config.TableName."
  value       = aws_dynamodb_table.workflows.name
}

output "table_arn" {
  value = aws_dynamodb_table.workflows.arn
}

output "queue_url" {
  description = "Task queue URL, for awsworld.Config.QueueURL."
  value       = aws_sqs_queue.tasks.id
}

output "queue_arn" {
  description = "Task queue ARN, for awsworld.SchedulerConfig.TargetARN."
  value       = aws_sqs_queue.tasks.arn
}

output "dead_letter_queue_url" {
  value = aws_sqs_queue.dead_letter.id
}

output "dead_letter_queue_arn" {
  value = aws_sqs_queue.dead_letter.arn
}

output "scheduler_group_name" {
  description = "Schedule group, for awsworld.SchedulerConfig.Group."
  value       = var.enable_scheduler ? aws_scheduler_schedule_group.workflows[0].name : null
}

output "scheduler_role_arn" {
  description = "Role Scheduler assumes to send to the queue, for awsworld.SchedulerConfig.RoleARN."
  value       = var.enable_scheduler ? aws_iam_role.scheduler[0].arn : null
}

output "worker_policy_arn" {
  description = "Managed policy for the queue worker role."
  value       = aws_iam_policy.worker.arn
}

output "api_policy_arn" {
  description = "Managed policy for the app-facing API role."
  value       = aws_iam_policy.api.arn
}

output "queues" {
  description = "Routed queues by name, for awsworld.Config.Queues."
  value = {
    for name, queue in aws_sqs_queue.routed : name => {
      url  = queue.id
      arn  = queue.arn
      fifo = var.queues[name].fifo
    }
  }
}

output "environment" {
  description = "Environment variables read by the example worker and API entrypoints."
  value = merge(
    tomap({
      WORKFLOW_TABLE     = aws_dynamodb_table.workflows.name
      WORKFLOW_QUEUE_URL = aws_sqs_queue.tasks.id
      # {"<name>":{"url":"...","arn":"...","fifo":false}}, for awsworld.Config.Queues.
      WORKFLOW_QUEUES = jsonencode({
        for name, queue in aws_sqs_queue.routed : name => {
          url  = queue.id
          arn  = queue.arn
          fifo = var.queues[name].fifo
        }
      })
    }),
    var.enable_scheduler ? tomap({
      WORKFLOW_QUEUE_ARN          = aws_sqs_queue.tasks.arn
      WORKFLOW_SCHEDULER_GROUP    = aws_scheduler_schedule_group.workflows[0].name
      WORKFLOW_SCHEDULER_ROLE_ARN = aws_iam_role.scheduler[0].arn
    }) : tomap({})
  )
}
