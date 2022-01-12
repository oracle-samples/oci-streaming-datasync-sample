# Copyright (c)  2022,  Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.


resource "oci_core_vcn" "export_DataSyncVCN" {
 
  cidr_blocks = [
    var.vnc_cidr_block,
  ]
  compartment_id = var.compartment_ocid

  display_name = "DataSyncVCN"


  
}



resource "oci_core_subnet" "export_Public-Subnet-DataSyncVCN" {
  
  cidr_block     = var.vnc_cidr_block
  compartment_id = var.compartment_ocid


  display_name = "Public Subnet-DataSyncVCN"


 
  prohibit_internet_ingress  = "false"
  prohibit_public_ip_on_vnic = "false"
  route_table_id             = oci_core_vcn.export_DataSyncVCN.default_route_table_id
  security_list_ids = [
    oci_core_vcn.export_DataSyncVCN.default_security_list_id,
  ]
  vcn_id = oci_core_vcn.export_DataSyncVCN.id
}




resource "oci_core_default_route_table" "export_Default-Route-Table-for-DataSyncVCN" {
  compartment_id = var.compartment_ocid

  display_name = "Default Route Table for DataSyncVCN"

  manage_default_resource_id = oci_core_vcn.export_DataSyncVCN.default_route_table_id
  route_rules {

    destination       = "0.0.0.0/0"
    destination_type  = "CIDR_BLOCK"
    network_entity_id = oci_core_internet_gateway.export_Internet-Gateway-DataSyncVCN.id
  }
}



resource "oci_core_default_security_list" "export_Default-Security-List-for-DataSyncVCN" {
  compartment_id = var.compartment_ocid

  display_name = "Default Security List for DataSyncVCN"
  egress_security_rules {

    destination      = "0.0.0.0/0"
    destination_type = "CIDR_BLOCK"
    
    protocol  = "all"
    stateless = "false"
   
  }


ingress_security_rules {

    icmp_options {
      code = "-1"
      type = "3"
    }
    protocol    = "1"
    source      = var.vnc_cidr_block
    source_type = "CIDR_BLOCK"
    stateless   = "false"

  }
  ingress_security_rules {

    protocol    = "6"
    source      = "0.0.0.0/0"
    source_type = "CIDR_BLOCK"
    stateless   = "false"
    tcp_options {
      max = "443"
      min = "443"

    }

  }
  manage_default_resource_id = oci_core_vcn.export_DataSyncVCN.default_security_list_id
}

resource "oci_core_internet_gateway" "export_Internet-Gateway-DataSyncVCN" {
  compartment_id = var.compartment_ocid

  display_name = "Internet Gateway-DataSyncVCN"
  enabled      = "true"

  vcn_id = oci_core_vcn.export_DataSyncVCN.id
}



