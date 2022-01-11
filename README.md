****Data Syncing using OCI - Streaming Pattern****

**Introduction**


There are many instances where there is a need for syncing data from source application/s to target application/s. 
A sample scenario is a custom mobile app/ web application developed to perform transactions on SaaS data. In this case, the mobile/web application fetches data from SaaS. Mobile/Web application user will perform transactions on  this data and those transactions  should be pushed to SaaS. Here source application is the custom mobile/web application and target application is SaaS. Another case could be the integration of external systems with SaaS, with a need to continuously send data from external systems to SaaS.

Regardless of what the source or target application is, it is ideal to have a middle tier using OCI native services that handles the data flow due to a number of reasons —

1.	Reduced load on the source application in terms of data sync operations, retrials and error handling.
2.	Ability to persist data and perform retrials in case of failure.
3.	Ability to handle data sync from multiple source & target applications from a single middle tier.
4.	Ability to transform or filter messages at the middle tier before sending to target application.
5.	Easy monitoring/reporting of the data flow. 
6.	Ability to fire notifications in case of failures.
7.	Ability to have a consolidated view of the data sync activities and error cases.
8.	Ability to enable data syncing in a publish-subscribe asynchronous model. 
9.	Allow source application to continue with data syncing operation even if target application is down, say for maintenance. 
10.	Ability to use centralized metrics and logging features.
11.	Ability to scale the middle tier based on the data load and processing requirements. 


This solution shows how you can Oracle Cloud Infrastructure (OCI) cloud native services to build a serverless data syncing solution. There are various approaches to build a data sync middle tier using OCI. This one uses Streaming, API Gateway, Functions, Service Connector Hub, Vault, OCI Registry, Notifications and Object Storage.

Choosing OCI Cloud Native Services as middle tier has the following benefits,
1.	They are based on open source and standards.
2.	They have built-in management capabilities. So development teams can focus on building competitive features and spend less time installing, patching, and maintaining infrastructure.
3.	Availability of good pricing models.
4.	They are highly secure, scalable, durable and reliable.

**Bringing the services together**


_Streaming_

There are 2 types of streams used.
•	A stream for storing the posted data from the source application/s.  Let’s call it a Data Stream .
•	A stream or streams for storing errored data. Posting of data to target application/s can error out due to multiple reasons, like server unavailability, data inconsistency, error on the server side while processing and so forth. Some of these errors are recoverable, say an error occurred due to server unavailability is recoverable when server is available. Some of them would be unrecoverable, i.e. the processing of data will not be successful even after several retrials. It is important to categorize and re-process errored messages based on the error type to avoid data loss. In the sample code developed for this pattern, retrial is based on the REST API response code. Please note that, the error type and retrial decision is based on the business use case and using REST API response code may not be suitable for all business cases.
The data will be moved from Data Stream  to Error streams based on the error type and classification. 

_Functions_

3 Functions are used in this pattern. 

•	PopulateDataStreamFunction → This Function is used to populate the DataStream. It is invoked when the Source Application/s post data to the REST API exposed using API Gateway. 
•	ProcessDataStreamFunction → This Function reads the Data Stream  messages and calls the target application’s API. If there is a failure in target application API call, the messages are sent to Error Streams. The Error Streams to use are configurable at the Function Application level and the Function reads them from the Application configuration at run time. This gives additional flexibility in defining the error conditions and the streams to which messages are stored based on the business case. 
•	RetryFunction → This Function retries the messages in Error Streams. This Function is exposed as a public API using an API Gateway. The exposed API can be invoked as a batch process or on an ad-hoc basis, to reprocess the failed messages in any Error Stream. 

**Architecture**

**Installation**


_Pre-requisites_


1. Make sure you've setup your API signing key, installed the Fn CLI, completed the CLI configuration steps and have setup the OCI Registry you want to use.

2. Ensure Terraform is installed.

3. You have the Target application's REST API, Auth token and Json Payload for loading data to it.



_Creating the cloud artefacts in OCI cloud_

Download the files from the respository and navigate to location where you downloaded the files

Run Terraform to create all your resources in OCI. 

terraform init

terraform plan

terraform apply

This step creates all the resources in OCI , including the setup of a VCN, an API Gateway, uploading the Oracle Cloud Functions and creating a OCI Vault to store the Fusion ERP password. 


Log In to OCI console and validate whether all OCI resources are created



_Running the sample_


Contributing
We welcome all contributions to this sample and have a contribution guide for you to follow if you'd like to contribute.

Help
If you need help with this sample, please log an issue within this repository and the code owners will help out where we can.

Copyright (c) 2021, Oracle and/or its affiliates. Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
