### What is BigDime  : 
BigDime is code name for Data Ingestion Eco System Project. it stands for Big Data Ingestion Made Easy. it consists of following things.

Core Data Ingestion Framework - that leverages apache flume 

Handler Orchestration framework  - that allows to building different handlers to ingest different data sources and formats.

Data Adaptors - Configuration  driven group of handlers for ingesting any particular data source. ( an example :  click stream,preferences..etc)

Metastore  - Core framework for metadata management, for immediate querablity of ingested data with appropriate partitions.

Monitoring - Framework for monitoring  system failures and alerts on them.

Management Console - console to providing visibility to available data and running jobs/ applications status.

Getting Started
=======

1. Clone the BigDime repository to, say /opt/bigdime/repo.
2. From the /opt/bigdime/repo path, run "mvn clean package". This may take a couple of minutes to complete.
3. Verify that bigdime-dist-${version}-SNAPSHOT-bin.tar.gz exists in /opt/bigdime/repo/dist/target/.
4. Navigate to /opt/bigdime and untar the file using following command:
	4. tar xvf /opt/bigdime/repo/dist/target/bigdime-dist-${version}-SNAPSHOT-bin.tar.gz
	5. Above command will extract following files inside bigdime-dist-${version}-SNAPSHOT directory:
		6. bigdime-adaptor-${version}-SNAPSHOT.jar
		6. config/application.properties.template
		7. config/log4j.properties.template
		7. config/META-INF/adaptor.json.template.file
		8. config/META-INF/adaptor.json.template.kafka
		8. logs/
9. Create adaptor.json in /opt/bigdime/bigdime-dist-${version}-SNAPSHOT/config/META-INF/ to complete the adaptor configuration, use one of the adaptor.json.template files as a starting point.
10. Create application.properties in /opt/bigdime/bigdime-dist-${version}-SNAPSHOT/config/, use application.properties.template file as a starting point.
11. Create log4j.properties in /opt/bigdime/bigdime-dist-${version}-SNAPSHOT/config/, use log4j.properties.template file as a starting point.
12. Run the following command from /opt/bigdime/bigdime-dist-${version}-SNAPSHOT:
java -jar -Dloader.path=./config/ -Denv.properties=application.properties bigdime-adaptor-${version}-SNAPSHOT.jar

