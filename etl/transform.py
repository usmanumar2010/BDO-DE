
import pyspark.sql.functions as f
from pyspark.sql.functions import to_timestamp,element_at, hour, minute,  to_date
from pyspark.sql import SQLContext

class sqlTransfomation:
    def __init__(self,spark,df):
        self.sqlContext = SQLContext(spark)
        df.registerTempTable("retail")
    def avg_time_flyer_per_user(self):
        return self.sqlContext.sql('''
        
                    SELECT  t.user_id,ROUND(AVG(COALESCE((bigint(t.precedeing_time) - bigint(t.timestamp))/60,0)),2) average_time_minutes 
                    from 
                    (select *,LEAD(timestamp,1) over(Partition by user_id,event order by timestamp) as precedeing_time  from retail where event='flyer_open') t  
                    group by t.user_id
                  
                   ''')
    def avg_time_on_each_flyer_per_user(self):
        return self.sqlContext.sql('''  SELECT  t.user_id,t.flyer_id,ROUND(AVG((bigint(t.preceding_time) - bigint(t.timestamp))/60),2) average_time_minutes 
                                        from 
                                        (select *,LEAD(timestamp,1) over(Partition by user_id,event order by timestamp) as preceding_time  from retail where event='flyer_open') t  
                                        group by t.user_id,t.flyer_id
                   ''')




class Transform(sqlTransfomation):
    def __init__(self,df,sparkContext):
        self.dataframe=df
        self.spark=sparkContext

    def apply_transformations(self):
        super().__init__(self.spark, self.dataframe)

        t1=self.avg_time_flyer_per_user()
        t2=self.avg_time_on_each_flyer_per_user()


        return t1,t2


