from sedona.spark import *
from pathlib import Path
from pyspark.sql.functions import col, lower, lit, split


def clean_france_commune(task_id:str, sedona_session, fr_commune_file_path:str, data_output_dir:str):
    print(f"Staring task: {task_id}")
    fr_commune_table_name = "fr_commune"
    paris_polygon_table_name = "paris_polygon"
    distance = 8000
    try:
        print(f"Read fr_commune data from: {fr_commune_file_path}")
        fr_commune_raw = sedona_session.read.format("geoparquet").load(fr_commune_file_path)
        fr_commune_df = fr_commune_raw.select("geometry", "nom", "insee").withColumn("name", lower(col("nom"))).drop("nom")

        fr_commune_df.createOrReplaceTempView(fr_commune_table_name)
        paris_polygon_df = fr_commune_df.filter(col("name") == "paris").withColumn("centroid", ST_Centroid(col("geometry")))
        paris_polygon_df.createOrReplaceTempView(paris_polygon_table_name)

        query = f"SELECT fr.name FROM {paris_polygon_table_name} p, {fr_commune_table_name} fr WHERE ST_DWithin(p.centroid, fr.geometry,{distance},true)"
        filtered_commune_df = sedona_session.sql(query)
        target_commune = [row["name"] for row in filtered_commune_df.select("name").collect()]
        target_commune_df = fr_commune_df.filter(col("name").isin(target_commune))
        print(f"Get target commune in the {distance} meter radius of paris center")
        target_commune_out_path = f"{data_output_dir}/target_commune_cleaned"
        target_commune_df.coalesce(1).write.mode("overwrite").format("geoparquet").option("geoparquet.version", "1.1.0").save(target_commune_out_path)
        print(f"Successfully write the target commune at {target_commune_out_path}")
    except Exception as e:
        print(f"Can't generate the target commune: {e}")
    finally:
        print(f"Task {task_id} ended")

def clean_france_hospitals(task_id:str, sedona_session, ile_france_pbf_path:str, data_output_dir:str):
    raw_hospital_table_name = "raw_hospital"
    long_col_name = "longitude"
    lat_col_name = "latitude"
    source_epsg_code = "epsg:4326"
    target_epsg_code = "epsg:4326"
    target_geo_col_name = "location"
    print(f"Staring task: {task_id}")
    try:
        print(f"Read ile de france data from: {ile_france_pbf_path}")
        spark = sedona_session.getActiveSession()
        osm_df = spark.read.parquet(ile_france_pbf_path)
        raw_hospital_df = osm_df.select("id", "latitude", "longitude", "tags").where("element_at(tags, 'amenity') in ('hospital', 'clinic')")
        raw_hospital_df.createOrReplaceTempView(f"{raw_hospital_table_name}")
        hospital_geo_df = sedona_session.sql(f"""
            SELECT id, tags,
            ST_Transform(ST_Point(CAST({long_col_name} AS Decimal(24,20)), CAST({lat_col_name} AS Decimal(24,20))), '{source_epsg_code}', '{target_epsg_code}') AS {target_geo_col_name} from {raw_hospital_table_name}""")
        print(hospital_geo_df.show())
        hospital_out_path = f"{data_output_dir}/hospital_cleaned"
        hospital_geo_df.coalesce(1).write.mode("overwrite").format("geoparquet").option("geoparquet.version",
                                                                                          "1.1.0").save(
            hospital_out_path)
        print(f"Successfully write the target hospitals at {hospital_out_path}")
    except Exception as e:
        print(f"Can't generate the target hospital: {e}")
    finally:
        print(f"Task {task_id} ended")

def count_hospitals_in_each_commune(task_id:str, sedona_session, data_output_dir:str):
    print(f"Staring task: {task_id}")
    target_commune_table_name = "target_commune"
    hospital_table_name = "hospital_geo"
    hospital_per_commune_table_name = "commune_hospital"
    target_commune_cleaned_path = f"{data_output_dir}/target_commune_cleaned"
    hospital_cleaned_path = f"{data_output_dir}/hospital_cleaned"
    hospital_count_out_path = f"{data_output_dir}/hospital_count"
    kepler_map_path = f"{data_output_dir}/hospital_count_map.html"
    try:
        hospital_cleaned_df = sedona_session.read.format("geoparquet").load(hospital_cleaned_path)
        hospital_cleaned_df.createOrReplaceTempView(f"{hospital_table_name}")

        target_commune_cleaned_df = sedona_session.read.format("geoparquet").load(target_commune_cleaned_path)
        target_commune_cleaned_df.createOrReplaceTempView(f"{target_commune_table_name}")

        # join hospital and commune with condition ST_Contains
        commune_hospital_df = sedona_session.sql(f"\n"
                                         f"             select \n"
                                         f"             tc.name, tc.geometry, h.id\n"
                                         f"             FROM {target_commune_table_name} tc, {hospital_table_name} h \n"
                                         f"             WHERE \n"
                                         f"             ST_Contains(tc.geometry, h.location)\n"
                                         f"            ")

        commune_hospital_df.show()
        commune_hospital_df.createOrReplaceTempView(f"{hospital_per_commune_table_name}")
        # count hospital in each commune
        hospital_count_df = sedona_session.sql(
            f"SELECT c.name, c.geometry, count(*) as hospital_count "
            f"FROM {hospital_per_commune_table_name} c "
            f"GROUP BY c.name, c.name, c.geometry "
            f"sort by hospital_count desc")
        hospital_count_df.coalesce(1).write.mode("overwrite").format("geoparquet").option("geoparquet.version",
                                                                                          "1.1.0").save(
            hospital_count_out_path)
        # generate data viz map
        sedona_kepler_map = SedonaKepler.create_map(df=hospital_count_df, name="hospital_count_df")
        sedona_kepler_map.save_to_html(file_name=kepler_map_path)
        print(f"Successfully write the hospital count per commune at {hospital_count_out_path}")
    except Exception as e:
        print(f"Can't calculate the hospital count per commune: {e}")
    finally:
        print(f"Task {task_id} ended")



def main():
    # get the project root dir
    project_root_dir = Path.cwd().parent.parent.parent
    print(f"project root dir: {project_root_dir}")

    linux = True
    win_root_dir = "C:/Users/PLIU/Documents/ubuntu_share/data_set"
    lin_root_dir = "/mnt/hgfs/ubuntu_share/data_set"
    if linux:
        data_root_dir = lin_root_dir
    else:
        data_root_dir = win_root_dir

    print(f"data root dir: {data_root_dir}")

    fr_commune_file_path = f"{data_root_dir}/kaggle/geospatial/communes_fr_geoparquet"

    ile_france_pbf_path = f"{data_root_dir}/geo_spatial/ile-de-france-geo-parquet"


    data_output_dir = f"{project_root_dir}/data/tmp"
    print(f"data output dir: {data_output_dir}")

    # build a sedona session (sedona = 1.6.1)
    jar_folder = Path(f"{project_root_dir}/jars/sedona-35-213-161")
    jar_list = [str(jar) for jar in jar_folder.iterdir() if jar.is_file()]
    jar_path = ",".join(jar_list)

    # build a sedona session (sedona = 1.6.1) offline
    config = SedonaContext.builder() \
        .master("local[*]") \
        .config('spark.jars', jar_path). \
        getOrCreate()

    sedona = SedonaContext.create(config)
    sc = sedona.sparkContext

    sc.setSystemProperty("sedona.global.charset", "utf8")

    # step1: clean the raw france commune data
    clean_france_commune("clean_france_commune",sedona, fr_commune_file_path, data_output_dir)

    # step2: clean the osm france hospital data
    clean_france_hospitals("clean_hospitals",sedona,ile_france_pbf_path, data_output_dir)

    # step3: calculate hospital counts per commune
    count_hospitals_in_each_commune("count_hospital_per_commune", sedona, data_output_dir)



if __name__=="__main__":
    main()
