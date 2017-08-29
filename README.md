This project is used to create sub directories recursively under one directory.
It is useful to diagnose the potential performance issue before HDP-2.6.1.29-3.
Please check https://hortonworks.jira.com/browse/BUG-86615 for more details

Compile:
  mvn clean package
Deployment:
  copy create_dir-1.0-SNAPSHOT.jar lib target/classes/conf to the destination.
Usage:
 hadoop jar create_dir-1.0-SNAPSHOT.jar <HDFS_URL> <PARENT_DIRECTORY> <Thread Count> <SUBDIR_COUNT> <SUBDIR_DEPTH> <Do Create Directory or dry run>
 Example:
 hadoop jar create_dir-1.0-SNAPSHOT.jar hdfs://yourcluster/ /user/appcarpo  128 1468576 2 1
 this example will use 128 threads to send mkdir request to namenode.it will create 1468576 sub directories under /user/appcarpo
 and again create sub-subdirectories under each sub directories of /user/appcarpo , because SUBDIR_DEPTH is 2.
 