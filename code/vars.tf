# Copyright (c)  2022,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.



variable "compartment_ocid" {

  description = "The compartment ocid where you are creating the services."
  type        = string
}

variable "vnc_cidr_block" {
  type    = string
  default = "10.0.0.0/16"
}


variable "notification_email_id" {
  description = "Email address to configure in notififcations service to send notifications in case of error in data syncing."
  type        = string

}
variable "key_management_endpoint" {
  description = "The service endpoint to perform management operations against. "
  type        = string

}




