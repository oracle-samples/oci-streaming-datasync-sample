# Copyright (c)  2021,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.


resource "null_resource" "deploy_RetryFunction" {
  
 depends_on = [ oci_functions_application.export_DataSyncApplication]
  provisioner "local-exec" {
    working_dir = "${abspath(path.root)}/RetryFunction"
    command = <<-EOC
      fn -v deploy --app DataSyncApplication
      
    EOC
  }
}
data "oci_functions_functions" "test_deploy_RetryFunction" {
depends_on = [null_resource.deploy_RetryFunction]
    application_id=oci_functions_application.export_DataSyncApplication.id
    display_name   = "retryfunction"
    
  
}

resource "null_resource" "deploy_ReadDataStreamFunction" {
  depends_on = [ oci_functions_application.export_DataSyncApplication]
 
  provisioner "local-exec" {
    working_dir = "${abspath(path.root)}/ReadDataStreamFunction"
    command = <<-EOC
      fn -v deploy --app DataSyncApplication
      
    EOC
  }
}

data "oci_functions_functions" "test_deploy_ReadDataStreamFunction" {
  depends_on = [null_resource.deploy_ReadDataStreamFunction]
    application_id=oci_functions_application.export_DataSyncApplication.id
    display_name   = "readdatastreamfunction"
    
  
}

resource "null_resource" "deploy_PopulateDataStreamFunction" {
  
 depends_on = [ oci_functions_application.export_DataSyncApplication]
  provisioner "local-exec" {
    working_dir = "${abspath(path.root)}/PopulateDataStreamFunction"
    command = <<-EOC
      fn -v deploy --app DataSyncApplication
      
    EOC
  }
}
data "oci_functions_functions" "test_deploy_PopulateDataStreamFunction" {
     depends_on = [null_resource.deploy_PopulateDataStreamFunction]
    application_id=oci_functions_application.export_DataSyncApplication.id
    display_name   = "populatedatastreamfunction"
    
  
}
output "list_populatedatastreamfunction" {
        value = data.oci_functions_functions.test_deploy_PopulateDataStreamFunction.functions.0.id
} 


