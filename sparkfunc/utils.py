from pyspark.sql.types import *
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import StructType , StructField, LongType, StringType, ArrayType,FloatType,TimestampType
from pyspark.sql.functions import year
from pyspark.sql.functions import to_date
from pyspark.sql.functions import month
import pyspark.sql.functions as F
import datetime
import pandas as pd



def show_missing_values(df):
  ''' 
  It computes missing values for each column.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  
  df=(df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])).toPandas()
  df=df.T
  df["column_names"]=df.index
  return (df)





def count_distinct_values(df):
    df=(df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns))).toPandas()
    df=df.T
    df["column_names"]=df.index
    return (df)




def count_duplicate_values(df):
  ''' 
  It computes duplicate values for each column.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  
    lis=[]  
    names=[]
    for c in df.columns:
        lis.append((df.count()-(df.dropDuplicates(subset=[c]).count())))
        names.append(c)
    dup=pd.DataFrame(lis)
    dup['columns_names']=names
    return display(dup)





def count_duplicate_rows(df):
  ''' 
  It computes duplicate rows for dataframe.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  
    qwry= df.groupBy(df.columns).count().where(f.col('count') > 1)
    qwry=qwry.withColumn("product_cnt", qwry['count']-1)
    return display(qwry.where(f.col('product_cnt') >= 1).select(f.sum('product_cnt')))



def percentage_duplicate_rows(df):
  ''' 
  It computes percentage of duplicate rows.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  
    qwry= df.groupBy(df.columns).count().where(F.col('count') > 1)
    qwry=qwry.withColumn("duplicate_cnt", qwry['count']-1)
    qwry=qwry.where(F.col('duplicate_cnt') >= 1).select(F.sum('duplicate_cnt'))
    return display(qwry.withColumn('percentage',(qwry['sum(duplicate_cnt)']/df.count())*100))




def count_distinct_rows(df):
  return df.distinct().count()




def show_fill_rate(df):
  ''' 
  It computes fill rate for each column.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  
    df=df.select([(((count(when(col(c).isNotNull(), c)).alias(c))/df.count())*100).alias(c) for c in df.columns]).toPandas()
    df=df.T
    df["column_names"]=df.index
    return (df)





def distinct_values_each_column(df):
  for col_name in df:
    df=df.select(col_name).distinct().collect()
  return display(df)





def split_date_col(s):
  ''' 
  It splits date into individual properties of its own.
  
  Parameters: string,  
  Return: string
  '''
  
  split_date=split(s['date'], '-')     
  s= s.withColumn('Year', split_date.getItem(0))
  s= s.withColumn('Month', split_date.getItem(1))
  s= s.withColumn('Date_of_month', split_date.getItem(2))
  s=s.withColumn('year_month',concat(col('Year'),col('month')))
  s=s.withColumn("Week_Day", date_format(col("date"), "EEE"))
  return s



def date_function(date_list:ArrayType(DateType())):
  date_list=list(date_list)
  m=len(date_list)
  gap_list=[]
  if m==1:
    gap_list.append((0))
  else:
    for j in range(m-1):
      gap_list.append((pd.to_datetime(date_list[j+1]).date()-pd.to_datetime(date_list[j]).date()).days)
  return gap_list



def epoch_to_date(df):
  df=df.withColumn('epoch_clean',F.when(F.col('epoch')>10000000000, (F.col('epoch')/1000).cast("Int")).otherwise(F.col('epoch')))
  df=df.select("*",F.from_unixtime((df.epoch_clean.cast('bigint'))).cast('timestamp').alias('epoch_date'))
  df=df.withColumn('date_only',F.date_format(F.col('epoch_date'),"yyyy-MM-dd HH:mm:ss").cast("date"))
  return df




def show_missing_values(df):
  
  ''' 
  It computes missing values for each column.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  
  df=(df.select([count(when((col(c).isNull()) | (col(c)=='(null)'), c)).alias(c) for c in df.columns])).toPandas()
  df=df.T
  df=df.rename(columns={0:'Missing_Values'})
  df.insert(loc=0, column='Column_Names', value=df.index)
  return df 


def data_quality_analysis(df):
  
  ''' 
  It computes total row count, distinct row count, and duplicate row count for entire dataframe
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Spark Dataframe
  '''
  
  count=df.count()
  distinct_count=df.distinct().count()
  duplicate_count=count-(df.dropDuplicates().count())
  duplicate_percentage=(duplicate_count/count)*100
  lis=[[count, distinct_count, duplicate_count, duplicate_percentage]]
  data=spark.createDataFrame(data=lis)
  #data=pd.DataFrame(lis, columns=['Total_rows_count', 'Distinct_rows_count', 'Duplicate_rows_counts', 'Percentage_duplicate_row'])
  return data  
