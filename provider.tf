# Copyright (c)  2021,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
provider "oci" {
  tenancy_ocid = "ocid1.tenancy.oc1..aaaaaaaadymht6gout6mc5yhsqo4syry4inm2kc44s5o4s7c3re2ylgs66xa"
  user_ocid = "ocid1.user.oc1..aaaaaaaajci5gjunoesqd3o4sbe67rud5pod3m3gzcouibcmihy5g23fli3a" 
  private_key_path = "~/.oci/oci_api_key.pem"
  fingerprint = "d7:d6:af:d9:1f:8b:8b:06:f3:2e:52:15:3b:03:de:a7"
  region = "us-ashburn-1"
}