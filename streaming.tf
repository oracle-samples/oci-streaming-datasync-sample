# Copyright (c)  2021,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

resource oci_streaming_stream_pool export_DefaultPool {
  compartment_id = var.compartment_ocid
  custom_encryption_key {
    kms_key_id = "<placeholder for missing required attribute>" #Required attribute not found in discovery, placeholder value set to avoid plan failure
  }
  defined_tags = {
  }
  freeform_tags = {
  }
  kafka_settings {
    auto_create_topics_enable = "false"
    log_retention_hours       = "24"
    num_partitions            = "1"
  }
  name = "DefaultPool"
  private_endpoint_settings {
    nsg_ids = [
    ]
   
  }

 
  lifecycle {
    ignore_changes = [custom_encryption_key[0].kms_key_id]
  }
}

resource oci_streaming_stream export_ServerUnavailableStream {
  compartment_id = var.compartment_ocid
 
  freeform_tags = {
  }
  name               = "ServerUnavailableStream"
  partitions         = "1"
  retention_in_hours = "24"
  
}

resource oci_streaming_stream export_UnrecoverableErrorStream {
  compartment_id = var.compartment_ocid
  
  freeform_tags = {
  }
  name               = "UnrecoverableErrorStream"
  partitions         = "1"
  retention_in_hours = "24"
 
}

resource oci_streaming_stream export_DataSyncStream {
  compartment_id = var.compartment_ocid
 
  freeform_tags = {
  }
  name               = "DataSyncStream"
  partitions         = "1"
  retention_in_hours = "24"
 
}

