# Copyright (c)  2022,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

resource oci_sch_service_connector export_ServerUnavailableToNotificationsConnector {
  
  compartment_id = var.compartment_ocid
  
 
  display_name = "ServerUnavailableToNotificationsConnector"
  freeform_tags = {
  }
  source {
    cursor {
      kind = "LATEST"
    }
    kind = "streaming"
   
    stream_id = oci_streaming_stream.export_ServerUnavailableStream.id
  }
  state = "ACTIVE"
  target {
  
    enable_formatted_messaging = "false"
   
    kind = "notifications"
   
    topic_id = oci_ons_notification_topic.export_ErrorTopic.id
  }
}

resource oci_sch_service_connector export_UnrecoverableErrorToNotificationsConnector {
  compartment_id = var.compartment_ocid
 
  description  = "UnrecoverableErrorToNotificationsConnector"
  display_name = "UnrecoverableErrorToNotificationsConnector"
  freeform_tags = {
  }
  source {
    cursor {
      kind = "LATEST"
    }
    kind = "streaming"
   
    stream_id = oci_streaming_stream.export_UnrecoverableErrorStream.id
  }
  state = "ACTIVE"
  target {
    
    enable_formatted_messaging = "false"
   
    kind = "notifications"
    
    topic_id = oci_ons_notification_topic.export_ErrorTopic.id
  }
}

resource oci_sch_service_connector export_UnrecoverableErrorToStorageConnector {
  compartment_id = var.compartment_ocid
  
  description  = "UnrecoverableErrorToStorageConnector"
  display_name = "UnrecoverableErrorToStorageConnector"
  freeform_tags = {
  }
  source {
    cursor {
      kind = "LATEST"
    }
    kind = "streaming"
   
    stream_id = oci_streaming_stream.export_UnrecoverableErrorStream.id
  }
  state = "ACTIVE"
  target {
    batch_rollover_size_in_mbs = "100"
    batch_rollover_time_in_ms  = "420000"
    bucket                     = "stream-error-bucket"
    
    kind = "objectStorage"
    
  }
}

resource oci_sch_service_connector export_DataSyncServiceConnector {
  compartment_id = var.compartment_ocid
  defined_tags = {
  }
  description  = "DataSyncServiceConnector"
  display_name = "DataSyncServiceConnector"
  freeform_tags = {
  }
  source {
    cursor {
      kind = "LATEST"
    }
    kind = "streaming"
    
    stream_id = oci_streaming_stream.export_DataSyncStream.id
  }
  state = "ACTIVE"
  target {
    
    function_id = data.oci_functions_functions.test_deploy_ReadDataStreamFunction.functions[0].id
    kind        = "functions"
    
  }
}

