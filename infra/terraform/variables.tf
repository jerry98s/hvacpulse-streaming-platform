variable "prefix" {
  description = "Short lowercase prefix for all resource names (e.g. hvacpulse)."
  type        = string
  default     = "hvacpulse"
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "southeastasia"
}

variable "eventhub_partition_count" {
  description = "Partitions for the telemetry Event Hub; match per-equipment keying."
  type        = number
  default     = 8
}

variable "eventhub_message_retention_days" {
  description = "Kafka-side hot retention. Expire only after Bronze confirmation."
  type        = number
  default     = 7
}

variable "tags" {
  type    = map(string)
  default = { project = "hvacpulse", environment = "dev" }
}
