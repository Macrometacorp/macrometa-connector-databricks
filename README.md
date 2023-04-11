# Macrometa Databricks Connector

The Macrometa Spark Connector for [Databricks](https://www.databricks.com/) is a versatile and efficient integration tool that enables users to seamlessly connect Macrometa's real-time data [streams](https://www.macrometa.com/docs/streams/) and [collections](https://www.macrometa.com/docs/collections/) with Apache Spark within the Databricks environment. This comprehensive connector facilitates the ingestion, processing, and analysis of both streaming and batch data by leveraging Spark's advanced capabilities, allowing users to derive valuable insights and make data-driven decisions.

With its simple installation process and compatibility with Databricks Runtime and Scala, the Macrometa Spark Connector offers two main components: a Streaming Data Connector and a Collection Data Connector. 

The Streaming Data Connector handles real-time data streams, while the Collection Data Connector focuses on batch data processing from Macrometa collections. 

1. Source and Target operations for collections can be executed using the format `com.macrometa.spark.collection.MacrometaTableProvider`
2. Source and Target operations for streams can be executed using the format `com.macrometa.spark.stream.MacrometaTableProvider`

## Prerequisites

- Databricks Runtime 11.3 LTS(with Apache Spark 3.3.0)
- Scala 2.12 or later
- Macrometa account with access to streams

## How to install the Macrometa Databricks Connector

1. Clone the Macrometa Spark Streaming Connector project from GitHub:

- git clone https://github.com/Macrometacorp/databricks-connector.git

2. Change to the project directory:

- cd databricks-connector

3. Build the connector JAR using Gradle:

- ./gradlew clean shadowJar

4. The generated JAR file named 'macrometa-spark-dataconnector.jar' will be located in the `build/libs` directory. Upload the JAR file to your Databricks workspace using the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) or the Databricks UI.

5. Attach the JAR file to your Databricks cluster by following the instructions in the [Databricks documentation](https://docs.databricks.com/libraries/cluster-libraries.html#install-a-library-on-a-cluster).

## How to use the Macrometa Databricks Connector

Detailed steps on how to use collections and streams connectors are listed bellow:
1. [Getting started with Macrometa Databricks Stream Data Connector](GETTING_STARTED_WITH_STREAM_DATA_CONNECTOR.md)
2. [Getting started with Macrometa Databricks Collection Data Connector](GETTING_STARTED_WITH_COLLECTION_DATA_CONNECTOR.md)