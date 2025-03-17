import os

class Loader:
    def __init__(self, file_path, spark):
        self.file_path = file_path
        self.spark = spark
        self.dict_df = {
            "Features": None,
            "logic": None,
            "Mfrs": None,
            "photogallery" : None,
            "pkgs": None,
            "Trims": None
        }

    def load_data(self):
        for key in self.dict_df:

            df = self.spark.read \
                    .option('header', 'true') \
                    .csv(self.file_path + "/" + key + ".csv")
                    # .limit(10000)
            self.dict_df[key] = df
        return self.dict_df