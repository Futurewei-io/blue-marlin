### Prerequisites
Gradle runs on all major operating systems and requires only a Java JDK or JRE version 8 or higher to be installed. To check, run java -version:

###  Download and Install
Download code using following git command:

git clone -b opensource-1.0.1 http://10.124.206.248:30080/device-cloud-service/common-pps-ims-tmp.git

go to directory ../PPS-IMS/FIBased/IMService and run gradle war to build war file.

The war file will be created in 
.../PPS-IMS/FIBased/IMService/imsservice/build

Deploy the war file into a web server such as tomcat, weblogic, jboss or etc.

After starting tomcat, test the application using 
http://<server-ip>/imsservice/ping

###  Data Preparation

Three scripts are provided in .../PPS-IMS/FIBased/IMService/imsservice/src/main/resources/data to generate data documents for Elasticsearch.
More information is provided by each script.

###  Testing
Run unit tests with Gradle by using the following command

$ gradle clean test
