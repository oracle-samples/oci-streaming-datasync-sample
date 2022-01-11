# Copyright (c)  2021,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

data oci_objectstorage_namespace export_namespace {
  compartment_id = var.compartment_ocid
}
resource oci_objectstorage_bucket export_stream-error-bucket {
  access_type    = "NoPublicAccess"
  auto_tiering   = "Disabled"
  compartment_id = var.compartment_ocid
  
  
  
  metadata = {
  }
  name                  = "stream-error-bucket"
  namespace             = data.oci_objectstorage_namespace.export_namespace.namespace
  object_events_enabled = "false"
  storage_tier          = "Standard"
  versioning            = "Disabled"
}




