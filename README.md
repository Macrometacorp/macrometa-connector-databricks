# Macrometa Connector Databricks

The Macrometa Spark Connector for [Databricks](https://www.databricks.com/) is a versatile and efficient integration tool that enables users to seamlessly connect Macrometa's real-time data [streams](https://www.macrometa.com/docs/streams/) and [collections](https://www.macrometa.com/docs/collections/) with Apache Spark within the Databricks environment. This comprehensive connector facilitates the ingestion, processing, and analysis of both streaming and batch data by leveraging Spark's advanced capabilities, allowing users to derive valuable insights and make data-driven decisions.

With its simple installation process and compatibility with Databricks Runtime and Scala, the Macrometa Spark Connector offers two main components: a Streaming Data Connector and a Collection Data Connector. 

The Streaming Data Connector handles real-time data streams, while the Collection Data Connector focuses on batch data processing from Macrometa collections. 

1. Source and Target operations for collections can be executed using the format `com.macrometa.spark.collection.MacrometaTableProvider`
2. Source and Target operations for streams can be executed using the format `com.macrometa.spark.stream.MacrometaTableProvider`

## Prerequisites

- Databricks Runtime 11.3 LTS(with Apache Spark 3.3.0)
- Scala 2.12 or later
- Macrometa account with access to streams


## Considerations

- When mapping from a Macrometa Array to Spark Array, we are using ArrayType which is a collection data type that extends the DataType class which is a superclass of all types in Spark. All elements of ArrayType should have the same type of elements.
- During the process of auto inferring the schema, the Collection data connector fetches the first 50 documents from a collection and then determines the most frequent schema among them. Nevertheless, users are encouraged to specify their own schema definitions while creating the dataframe for enhanced accuracy.
- Similarly, in the context of auto inferring the schema, the Streaming data connector retrieves the earliest unconsumed message from a stream and utilizes the schema of that message as the foundational schema. Yet, it is recommended for users to specify their own schema definitions while creating the dataframe to achieve optimal outcomes.


## How to install the Macrometa Databricks Connector

1. Obtain the JAR file. You can obtain the JAR file for the connector through either of the following methods:

a. Using the Official GitHub Package: Download the pre-built JAR file from the official GitHub package for this repository. For example: app-0.0.1.jar. This is the recommended way for production usage.

b. Building from Source: Clone this repository by running git clone https://github.com/Macrometacorp/macrometa-connector-databricks.git, and then build the JAR file using Gradle. Open a terminal in the root folder of the project and execute the command: ./gradlew clean shadowJar. This method provides the latest code, but it may not be officially released, so it's not recommended for production environments.


2. The generated JAR file named 'macrometa-connector-databricks.jar' will be located in the `app/build/libs` directory. Upload the JAR file to your Databricks workspace using the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) or the Databricks UI.

3. Attach the JAR file to your Databricks cluster by following the instructions in the [Databricks documentation](https://docs.databricks.com/libraries/cluster-libraries.html#install-a-library-on-a-cluster).

## How to use the Macrometa Databricks Connector

Detailed steps on how to use collections and streams connectors are listed bellow:
1. [Getting started with Macrometa Databricks Stream Data Connector](GETTING_STARTED_WITH_STREAM_DATA_CONNECTOR.md)
2. [Getting started with Macrometa Databricks Collection Data Connector](GETTING_STARTED_WITH_COLLECTION_DATA_CONNECTOR.md)

