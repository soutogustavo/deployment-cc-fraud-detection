from pyspark.sql.types import StructType, StringType, StructField, IntegerType


# Schema for CC transactions - Assmuming features columns V1 to V28
transaction_schema = StructType(
    [StructField("transaction_id", StringType(), False)] +
    [StructField(f"V{c}", StringType(), True) for c in range(1,29)] +
    [StructField("Amount", StringType(), True)]
)

# Schema for predictions
predict_schema = StructType([
    StructField("is_fraud", IntegerType(), False)
])