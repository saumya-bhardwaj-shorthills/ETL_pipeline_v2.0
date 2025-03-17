from pyspark.sql.functions import (
    initcap, concat_ws, col, array, collect_set, collect_list, struct
)

class Transform:
    def __init__(self):
        pass
    
    def rename_trim_table(self, trim):
        trim_dataframe = (
            trim.withColumnRenamed('ManufacturerName', 'manufacturer')
                .withColumnRenamed('ModelYear', 'year')
                .withColumnRenamed('MSRP', 'msrp')
                .withColumn('model', concat_ws(" ", col("ModelName"), col("TrimName")))
                .withColumn('category', initcap(col("ProdType")))
                .withColumn('subcategory', initcap(col("ProdType")))
                .withColumn(
                    'description',
                    concat_ws(" ", col("manufacturer"), col("model"))
                )
                .select([
                    'ProdType',
                    'TrimId',
                    'ModelId',
                    'MakeId',
                    'manufacturer',
                    'model',
                    'year',
                    'msrp',
                    'description',
                    'TrimName',
                    'ModelName',
                    'category',
                    'subcategory'
                ])
        )
        return trim_dataframe
    
    def extract_country_from_feature_table(self, feature):
        feature_dataframe = (
            feature.filter(feature.AttributeName == 'Manufacturer Country')
                   .withColumnRenamed('Value', 'countries')
                   .withColumn('countries', array(col('countries')))
        )

        dropped_columns = ['PackageId', 'AttributeId', 'FeatureName', 'AttributeName']
        feature_dataframe = feature_dataframe.drop(*dropped_columns)
        
        return feature_dataframe
    
    def extract_options_for_trimId(self, feature):
        filtered_feature = (
            feature.filter(feature.Value == 'Optional')
                   .select(['TrimId', 'FeatureName'])
                   .groupBy('TrimId')
                   .agg(collect_set('FeatureName').alias('Options'))
        )
        return filtered_feature
    
    def extract_features_for_trimId(self, feature):
        filtered_feature = (
            feature.filter(feature.Value == 'Standard')
                   .select(['TrimId', 'FeatureName'])
                   .groupBy('TrimId')
                   .agg(collect_set('FeatureName').alias('Features'))
        )
        return filtered_feature
    
    def extract_meta_for_trimId(self, feature):
        filtered_meta = (
            feature.filter(feature.FeatureName == 'Identifiers')
                   .filter(feature.AttributeName == 'Data Provider')
                   .select(['TrimId', 'Value'])
                   .withColumnRenamed('Value', 'Meta')
        )
        return filtered_meta
    
    def extract_features_details_for_trimId(self, feature):
        filtered_feature = (
            feature
            .select(['TrimId', 'FeatureName', 'AttributeName', 'Value'])
            .groupBy('TrimId')
            .agg(
                collect_list(
                    struct('FeatureName', 'AttributeName', 'Value')
                ).alias('featureDetails')
            )
        )
        return filtered_feature

    def join_tables(self, df1, df2, col, join_type):
        try:
            dataframe_3 = df1.join(df2, on=col, how=join_type)
            return dataframe_3
        except Exception as e:
            print(e)
        return None
    
    def generate_output(self, final_data):
        rows = final_data.collect()
        result = []
 
        for row in rows:
            product_detail = {
                "general": {
                    "ProdType": row["ProdType"].upper() if row["ProdType"] else None,
                    "TrimId": row["TrimId"],
                    "ModelId": row["ModelId"],
                    "MakeId": row["MakeId"],
                    "manufacturer": row["manufacturer"],
                    "model": row["model"],
                    "year": row["year"],
                    "msrp": row["msrp"],
                    "description": row["description"],
                    "TrimName": row["TrimName"],
                    "ModelName": row["ModelName"],
                    "category": row["category"],
                    "subcategory": row["subcategory"],
                    "countries": row["countries"]
                },
                "meta": row["Meta"],
                "options": row["Options"],
                "features": row["Features"]
            }
            details_by_feature = {}
            if row["featureDetails"] is not None:
                for item in row["featureDetails"]:
                    fname = item["FeatureName"]
                    attr_name = item["AttributeName"]
                    val = item["Value"]
                    if fname not in details_by_feature:
                        details_by_feature[fname] = {}
                    details_by_feature[fname][attr_name] = val

            if row["Features"] is not None:
                for feature_name in row["Features"]:
                    product_detail[feature_name] = details_by_feature.get(feature_name, {})
            result.append(product_detail)
 
        return result
