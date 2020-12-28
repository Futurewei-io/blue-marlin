### Requirements

1. Java 8
2. Gradle 6.6.1

### Run
This project uses gradle as a build tool.
'gradle tasks' shows different tasks that are handle by jar and war plugins.
'gradle war' to compile and build the project.
Default value for webAppDirName is src/main/webapp

### War

The default behavior of the War task is to copy the content of src/main/webapp to the root of the archive. 
Your webapp directory may of course contain a WEB-INF sub-directory, which may contain a web.xml file. 
Your compiled classes are compiled to WEB-INF/classes. 
All the dependencies of the runtime [1] configuration are copied to WEB-INF/lib.


