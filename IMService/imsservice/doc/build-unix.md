### Prerequisites
Gradle runs on all major operating systems and requires only a Java JDK or JRE version 8 or higher to be installed. To check, run java -version:

###  Download and Install
Download code using following git command:

git clone -b master https://github.com/Futurewei-io/blue-marlin.git

go to directory ../IMService and run gradle war to build war file.

The war file will be created in 
.../IMService/imsservice/build

Deploy the war file into a web server such as tomcat, weblogic, jboss or etc.

After starting tomcat, test the application using 
http://<server-ip>/imsservice/ping

###  Data Preparation

Three scripts are provided in .../IMService/imsservice/src/main/resources/data to generate data documents for Elasticsearch.
More information is provided by each script.

###  Testing
Run unit tests with Gradle by using the following command

$ gradle clean test
