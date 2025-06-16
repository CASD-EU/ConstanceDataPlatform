# ConstanceDataPlatform

In this repository, we will use three seminar to illustrate how **CASD** treats `large-scale Geospatial data`.

## 1: Geospatial data calculation

There exist many tools and frameworks to work with geospatial data. In this seminar, we only shows `postgis` and `sedona`.

### 1.1 Postgis

### 1.2 Sedona and GeoParquet

**Apache Sedona (formerly known as GeoSpark)** is a cluster computing system for processing large-scale spatial data. 
Sedona extends existing cluster computing systems, such as `Apache Spark`, with a set of out-of-the-box 
distributed Spatial Datasets and Spatial SQL that `efficiently load, process, and analyze large-scale spatial 
data across machines`.

**GeoParquet** is an incubating [Open Geospatial Consortium (OGC)](https://ogc.org/) standard that adds interoperable 
geospatial types (e.g. Point, Line, Polygon) to Parquet. For more details, you can visit their official [website](https://geoparquet.org/)

#### 1.2.1 What sedona offers?

##### 1.2.1.1 **Distributed storage and computing**

Compare to other traditional solutions, sedona provides `distributed spatial datasets`(e.g. RDD, dataframe) and 
`distributed spatial queries`(e.g. range query, range join query, distance join query, K Nearest Neighbor query). This 
feature allows sedona to handle `large-scale Geospatial data`(upto PB).

##### 1.2.1.2 **Rich spatial objects and input formats**

Sedona provide predefined spatial objects:
 - point
 - line
 - polygone
 - multi-line
 - multi-polygone


Sedona supports various input formats:
 - **GeoParquet**
 - CSV/TSV
 - WKT/WKB
 - GeoJSON
 - GML1,GML2
 - KML
 - Shapefile
 - Geohash
 - GeoTIFF
 - NetCDF/HDF
 - GeoPackage

##### 1.2.1.3 **Rich spatial analytics tools**

Sedona supports multiple **CRS(Coordinate Reference System)** and data conversion between different `Spatial Reference System`.

Sedona provides rich data visualization api(e.g. pydeck, kepler).

Sedona provides geopandas compatibility.

Sedona supports various language api:
  - python
  - R
  - Java
  - Scala

### 1.3 Postgis vs Sedona pros and cons

#### 1.3.1 PostGIS

- Pros:
  * Easy to deploy and maintain
  * SQL-based geospatial data transformation and processing. 
  * Easy to integrate with traditional GIS tools(e.g. QGIS, GeoServer, ArcGIS).
  * Row oriented: better support for transactional data management 
- Cons:
  * Limited scalability for large datasets(Single-node performance constraints).
  * Storage and calculation are strongly coupled(Possible data duplication)


#### 1.3.2 Sedona

- Pros:
  * Can handle large-scale geospatial data processing (designed for distributed computing architecture).
  * Complete dissociation of data storage and processing. It supports various file system (e.g. HDFS, S3, etc.)
  * Easy to integrate with big data ecosystem (e.g. Hadoop, Flink, etc.).
  * Column oriented: better support for column oriented data transformation and analysis.
- Cons:
  * Hard to deploy and maintain in cluster mode(Single-node mode is easy to deploy and maintain).
  * Not designed for transactional data management. 
  * Complete dissociation of data storage and calculation.(Zero data duplication).

## 2. Metadata management

There are many metadata management tools. Most of them requires a licence, some of them are open source:
 - Atlas
 - Amundsen
 - DataHub
 - OpenMetadata
 - Marquez

In this seminar, we only compare Atlas and OpenMetadata.

If you are not familiar with the terminology such as data governance, data management and metadata management. 
Please go to this [page](https://github.com/pengfei99/DataGovernance/blob/main/README.md).

### 2.1 Atlas

**Apache Atlas** is a metadata management and data governance tool, which helps in tracking and managing 
mutations to dataset metadata. It provides a solution for collecting, processing, storing and maintaining 
metadata about data objects. It also boasts a rich REST interface for a multitude of operations, such as creating 
object types, entities insertion and searching.

For more details, you can visit their official [website](https://atlas.apache.org/) and their [github page](https://github.com/apache/atlas)(1.9k stars)

### 2.2 OpenMetadata

OpenMetadata is a unified platform for data
 - discovery
 - observability
 - governance 

It provides a `central metadata repository, in-depth lineage, and seamless team collaboration`.

For more details, you can visit their official [website](https://open-metadata.org/) and their [github page](https://github.com/open-metadata/OpenMetadata)(5.8k stars)


### 2.3 Atlas vs OpenMetadata

#### 2.3.1 Apache Atlas

- Pros:
   * Mature metadata management solution(e.g. metadata entities CRUD, data lineage, tag/glossary).
   * Supports for custom metadata types creation which can have primitive attributes, complex attributes, object references.

- Cons:
   * Legacy UI and less intuitive interface.
   * Lack of automatic metadata ingestion support. It only exists **Seven** hooks(e.g. HBase, Hive, Storm, etc.).
   * Requires significant maintenance outside Hadoop environments.
   * Access control policies are hard to set up.

#### 2.3.2 OpenMetadata

- Pros:
   * Mature metadata management solution(e.g. metadata entities CRUD, data lineage, tag/glossary).
   * Supports for custom metadata types creation which can have primitive attributes, complex attributes, object references.
   * Access control policies are easy to set up.
   * Provides many automatic metadata ingestion support. It provides **more than 50** hooks (e.g. MySQL, Postgres, Hive, Spark, etc.).
   * Modern UI and allow collaboration between different user role.

- Cons:
   * Requires significant maintenance if hosted on premise.

## 3. Workflow automation

### 3.1 NiFi


NiFi is a **dataflow automation tool** designed for `real-time, event-driven` data ingestion, routing, transformation, and monitoring.

For more details, you can visit their official [website](https://nifi.apache.org/) and their [github page](https://github.com/apache/nifi) (5k stars)

### 3.2 Airflow

Airflow is a **workflow orchestration tool** designed to programmatically schedule, process, and monitor `complex batch ETL workflow`.

For more details, you can visit their official [website](https://airflow.apache.org/) and their [github page](https://github.com/apache/airflow) (38.2k stars)

### 3.3 Nifi vs Airflow

#### 3.3.1 Apache NiFi

- Pros:
    * Best for real-time, event-driven dataflows. 
    * Easy to use with visual, no-code UI. 
    * Built-in data lineage and provenance tracking. 
    * Provides a huge amount pre-defined processors(https://nifi.apache.org/components/). No code or low code for
      simple data integration

- Cons:
    * Not ideal for complex batch workflows. Need to write custom processors with data transformation logic.
    * Hard to share data pipelines via template(https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#Create_Template).
    * Complex configuration for cluster mode (e.g. ZooKeeper) and requires more resources for high-volume dataflows.
 
Use Nifi, if 
 - You need event-driven data ingestion and processing.
 - Data movement and integration between systems are the main goals.

#### 3.3.2 Apache Airflow 

- Pros:
   * Best for batch data processing and workflow scheduling. 
   * Highly customizable through Python-based workflows(Directed Acyclic Graph).
   * Provides various operators for complex data transformation(e.g. Bash, python, mysql, etc.)
   * Support various executor(e.g. Celery, K8s)

- Cons:
   * Not designed for real-time data ingestion. 
   * Requires python coding skills to define workflows.

Use Apache Airflow, if
 - You need complex data transformation with various dependencies.
 - You need to handle large scale data sources.
 - Workflows are based on scheduled jobs or batch processing.

#### 3.3.3 Prefect






