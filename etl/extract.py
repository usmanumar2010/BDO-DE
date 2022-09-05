from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType,TimestampType

class Extract:

    def __init__(self,input_path,sparkContext):
        self.input_path=input_path
        self.spark=sparkContext

    def extract_data(self):
        customSchema = StructType([
                            StructField("timestamp", TimestampType(), True),
                            StructField("user_id", StringType(), True),
                            StructField("event", StringType(), True),
                            StructField("flyer_id", IntegerType(), True),
                            StructField("merchant_id", IntegerType(), True), ]
                        )
        return self.spark.read.format("csv") \
                            .option("header", "true") \
                            .schema(customSchema) \
                            .load(self.input_path)