from connection import Connection
from load import Loader
from transform import Transform
import json
from pipeline import Pipeline

def main():
    Cluster_IP = "spark://localhost:7077"
    application_name = "pipeline_v2.0"
    file_path = "file:///data-2/CRS Datafeed 2025-02-28 1"

    connector = Connection(Cluster_IP, application_name)
    spark = connector.connect_to_cluster()
    loader = Loader(file_path, spark)
    data = loader.load_data()
    pipeline = Pipeline(data)
    pipeline.ETL()
    connector.stop_cluster(spark)



if __name__ == "__main__":
    main()