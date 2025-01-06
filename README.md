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
  * Column oriented: better support for data transformation and analysis.
- Cons:
  * Hard to deploy and maintain in cluster mode.
  * Not designed for transactional data management. 

## 2. Metadata management

### 2.1 Atlas

### 2.2 Open Metadata

OpenMetadata is a unified platform for data
 - discovery
 - observability
 - governance 

It provides a `central metadata repository, in-depth lineage, and seamless team collaboration`.


## 3. Workflow automation

### 3.1 Nifi

### 3.2 Airflow

### 3.3 Nifi vs Airflow

#### Use Apache NiFi
 
Use Nifi, if 
 - You need real-time data ingestion and processing.
 - Data movement and integration between systems are the main goals.

#### Use Apache Airflow 

 - You need complex task orchestration with dependencies.
 - Workflows are based on scheduled jobs or batch processing.






