import os

class Load:
    def __init__(self,output_path):
            self.output_path=output_path

    def load_data(self,df,filename):
        path=os.path.join(self.output_path, filename+ '.csv' )

        df.coalesce(1)\
           .write.format("com.databricks.spark.csv")\
           .option("header", "true")\
           .save(path)

