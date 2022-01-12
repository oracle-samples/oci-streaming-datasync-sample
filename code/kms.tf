# Copyright (c)  2021,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.

resource oci_kms_vault export_DataSync_Vault {
  compartment_id = var.compartment_ocid
 
  display_name = "DataSync_Vault"
 
  
  vault_type = "DEFAULT"
}

resource oci_kms_key export_SyncDataEncryptionKey {
  compartment_id = var.compartment_ocid
  defined_tags = {
  }
  desired_state = "ENABLED"
  display_name  = "SyncDataEncryptionKey"
  freeform_tags = {
  }
  key_shape {
    algorithm = "AES"
    curve_id  = ""
    length    = "32"
  }
  management_endpoint = var.key_management_endpoint
  protection_mode     = "HSM"
 
}

resource oci_kms_key_version export_SyncDataEncryptionKey_key_version_1 {
  key_id              = oci_kms_key.export_SyncDataEncryptionKey.id
  management_endpoint = var.key_management_endpoint
 
}

