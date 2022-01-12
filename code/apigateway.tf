# Copyright (c)  2022,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

resource oci_apigateway_gateway export_SyncDataGateway {
 
 
  compartment_id = var.compartment_ocid
  
  display_name  = "SyncDataGateway"
  endpoint_type = "PUBLIC"
 
  network_security_group_ids = [
  ]
  response_cache_details {
   
    type = "NONE"
  }
  subnet_id = oci_core_subnet.export_Public-Subnet-DataSyncVCN.id
}

resource oci_apigateway_deployment export_SyncDataDeployment {
  compartment_id = var.compartment_ocid
 
  display_name = "SyncDataDeployment"
  freeform_tags = {
  }
  gateway_id  = oci_apigateway_gateway.export_SyncDataGateway.id
  path_prefix = "/stream"
  specification {
    logging_policies {
      
      execution_log {
        is_enabled = "true"
        log_level  = "INFO"
      }
    }
    request_policies {
     
    }
    routes {
      backend {
       
        function_id = data.oci_functions_functions.test_deploy_PopulateDataStreamFunction.functions[0].id
       
        type = "ORACLE_FUNCTIONS_BACKEND"
        
      }
      logging_policies {
       
        execution_log {
         
          log_level = ""
        }
      }
      methods = [
        "POST",
      ]
      path = "/sync"
      request_policies {
       
      }
      response_policies {
       
      }
    }
    routes {
      backend {
        
        function_id = data.oci_functions_functions.test_deploy_RetryFunction.functions[0].id
       
        type = "ORACLE_FUNCTIONS_BACKEND"
        
      }
      logging_policies {
       
        execution_log {
          
          log_level = ""
        }
      }
      methods = [
        "POST",
      ]
      path = "/retry"
      request_policies {
        
      }
      response_policies {
        
      }
    }
  }
}

