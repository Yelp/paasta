variable "name_prefix" {
    default = "clusterman"
}
variable "metric_types" {
    type = "list"
    default = ["metadata", "app_metrics", "system_metrics"]
}
variable "read_capacity" {
    default = 5
}
variable "write_capacity" {
    default = 5
}
variable "read_autoscaling_enabled" {
    default = "false"
}
variable "write_autoscaling_enabled" {
    default = "false"
}
variable "max_read_capacity" {
    default = 100
}
variable "max_write_capacity" {
    default = 100
}
