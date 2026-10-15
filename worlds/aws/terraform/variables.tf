variable "name" {
  description = "Name prefix for the table, queues, schedule group, roles, and policies."
  type        = string
  default     = "workflow"
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}

variable "point_in_time_recovery" {
  description = "Enable point-in-time recovery on the table."
  type        = bool
  default     = true
}

variable "deletion_protection" {
  description = "Protect the table from deletion. Workflow state lives only in this table."
  type        = bool
  default     = true
}

variable "visibility_timeout_seconds" {
  description = "Queue visibility timeout. Keep it above the worker Lambda timeout."
  type        = number
  default     = 360
}

variable "message_retention_seconds" {
  description = "How long the queue and dead-letter queue keep undelivered messages."
  type        = number
  default     = 1209600
}

variable "max_receive_count" {
  description = "Deliveries of one message before it moves to the dead-letter queue."
  type        = number
  default     = 10
}

variable "enable_scheduler" {
  description = "Create the EventBridge Scheduler group and role for deliveries due more than 15 minutes ahead."
  type        = bool
  default     = true
}

variable "worker_function_name" {
  description = "Name or ARN of the queue worker Lambda running awsworld.Queue. When set, the module creates its SQS event source mapping."
  type        = string
  default     = null
}

variable "batch_size" {
  description = "SQS messages per worker invocation."
  type        = number
  default     = 10
}

variable "maximum_concurrency" {
  description = "Maximum concurrent worker invocations from the queue (2-1000). Null leaves it unbounded."
  type        = number
  default     = null
}

variable "worker_role_name" {
  description = "IAM role of the queue worker Lambda. When set, the worker policy is attached to it."
  type        = string
  default     = null
}

variable "api_role_name" {
  description = "IAM role of the app-facing API (awsworld.HTTP). When set, the API policy is attached to it."
  type        = string
  default     = null
}
