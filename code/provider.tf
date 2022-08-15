# Copyright (c)  2022,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
provider "oci" {
  tenancy_ocid = "ocid1.tenancy.oc1..xxxxx"
  user_ocid = "ocid1.user.oc1..xxxxxxx" 
  private_key_path = "~/.oci/oci_api_key.pem"
  fingerprint = "xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx"
  region = "us-ashburn-1"
}