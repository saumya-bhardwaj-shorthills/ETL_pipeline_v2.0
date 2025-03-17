from pyspark.sql import SparkSession

class Connection:
    def __init__(self, Cluster_IP, application_name):
        self.Cluster_IP = Cluster_IP
        self.application_name = application_name

    def connect_to_cluster(self):
        # Put your configs in the builder chain:
        spark = (
            SparkSession.builder
            .master(self.Cluster_IP)
            .appName(self.application_name)

            # Set configs before creating the session
            .config("spark.network.timeout", "600s")
            .config("spark.shuffle.io.connectionTimeout", "600s")
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.sql.shuffle.partitions", 200)
            # Notice the correct property is spark.executor.memory
            .getOrCreate()
        )

        return spark
    
    def stop_cluster(self, spark):
        spark.stop()
