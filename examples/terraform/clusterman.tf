resource "aws_dynamodb_table" "state_table" {
    name = "${var.name_prefix}_cluster_state"
    read_capacity = 1
    write_capacity = 1
    hash_key = "state"
    range_key = "entity"
    attribute {
        name = "state"
        type = "S"
    }
    attribute {
        name = "entity"
        type = "S"
    }
    ttl {
        attribute_name = "expiration_timestamp"
        enabled = true
    }
}

resource "aws_dynamodb_table" "metric_tables" {
    count = "${length(var.metric_types)}"
    name = "${var.name_prefix}_${element(var.metric_types, count.index)}"
    read_capacity = "${var.read_capacity}"
    write_capacity = "${var.write_capacity}"
    hash_key = "key"
    range_key = "timestamp"
    attribute {
        name = "key"
        type = "S"
    }
    attribute {
        name = "timestamp"
        type = "N"
    }
    attribute {
        name = "gsi_partition"
        type = "N"
    }
    attribute {
        name = "app_timestamp"
        type = "S"
    }
    ttl {
        attribute_name = "expiration_timestamp"
        enabled = true
    }
    lifecycle {
        ignore_changes = ["read_capacity", "write_capacity"]
    }
    global_secondary_index {
        name = "metrics_key_lookup"
        hash_key = "gsi_partition"
        range_key = "app_timestamp"
        read_capacity = "${var.metric_types[count.index] == "app_metrics" ? var.read_capacity : 1}"
        write_capacity = "${var.metric_types[count.index] == "app_metrics" ? var.write_capacity : 1}"
        projection_type = "INCLUDE"
        non_key_attributes = ["key"]
    }
}

resource "aws_appautoscaling_target" "dynamodb_table_read_target" {
    count = "${var.read_autoscaling_enabled == "true" ? length(var.metric_types) : 0}"
    min_capacity = "${var.read_capacity}"
    max_capacity = "${var.max_read_capacity}"
    resource_id = "table/${var.name_prefix}_${element(var.metric_types, count.index)}"
    scalable_dimension = "dynamodb:table:ReadCapacityUnits"
    service_namespace = "dynamodb"
}

resource "aws_appautoscaling_policy" "dynamodb_table_read_policy" {
    count = "${var.read_autoscaling_enabled == "true" ? length(var.metric_types) : 0}"
    name               = "DynamoDBReadCapacityUtilization:${var.name_prefix}_${element(var.metric_types, count.index)}"
    policy_type        = "TargetTrackingScaling"
    resource_id        = "${element(aws_appautoscaling_target.dynamodb_table_read_target.*.resource_id, count.index)}"
    scalable_dimension = "${element(aws_appautoscaling_target.dynamodb_table_read_target.*.scalable_dimension, count.index)}"
    service_namespace  = "${element(aws_appautoscaling_target.dynamodb_table_read_target.*.service_namespace, count.index)}"

    target_tracking_scaling_policy_configuration {
      predefined_metric_specification {
        predefined_metric_type = "DynamoDBReadCapacityUtilization"
      }

      target_value = 70
    }
}

resource "aws_appautoscaling_target" "dynamodb_table_write_target" {
    count = "${var.write_autoscaling_enabled == "true" ? length(var.metric_types) : 0}"
    min_capacity = "${var.write_capacity}"
    max_capacity = "${var.max_write_capacity}"
    resource_id = "table/${var.name_prefix}_${element(var.metric_types, count.index)}"
    scalable_dimension = "dynamodb:table:WriteCapacityUnits"
    service_namespace = "dynamodb"
}

resource "aws_appautoscaling_policy" "dynamodb_table_write_policy" {
    count = "${var.write_autoscaling_enabled == "true" ? length(var.metric_types) : 0}"
    name               = "DynamoDBWriteCapacityUtilization:${var.name_prefix}_${element(var.metric_types, count.index)}"
    policy_type        = "TargetTrackingScaling"
    resource_id        = "${element(aws_appautoscaling_target.dynamodb_table_write_target.*.resource_id, count.index)}"
    scalable_dimension = "${element(aws_appautoscaling_target.dynamodb_table_write_target.*.scalable_dimension, count.index)}"
    service_namespace  = "${element(aws_appautoscaling_target.dynamodb_table_write_target.*.service_namespace, count.index)}"

    target_tracking_scaling_policy_configuration {
      predefined_metric_specification {
        predefined_metric_type = "DynamoDBWriteCapacityUtilization"
      }

      target_value = 70
    }
}

output "table_arns" {
    value = "${aws_dynamodb_table.metric_tables.*.arn}"
}
