from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType


def cast_features_to_double(data: DataFrame, features: list[str]) -> DataFrame:
    """
    Cast specified feature columns to DoubleType.
    Returns a new DataFrame with casted columns.
    """
    for col_name in features:
        data = data.withColumn(col_name, col(col_name).cast(DoubleType()))
   
    return data
