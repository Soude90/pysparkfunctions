from pyspark.sql.types import *
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import StructType , StructField, LongType, StringType, ArrayType,FloatType,TimestampType
from pyspark.sql.functions import year
from pyspark.sql.functions import to_date
from pyspark.sql.functions import month
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import datetime
import pandas as pd



def show_missing_values1(df):
  ''' 
  It computes missing values for each column.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  
  df=(df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])).toPandas()
  df=df.T
  df["column_names"]=df.index
  return (df)





def count_distinct_values1(df):
    '''
    Counts the distinct values of a dataframe 
     Parameters:Dataframe:Spark Dataframe,
     Return: Pandas Dataframe
    '''
    df=(df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns))).toPandas()
    df=df.T
    df["column_names"]=df.index
    return (df)




def count_duplicate_values1(df):
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





def count_duplicate_rows1(df):
  ''' 
  It computes duplicate rows for dataframe.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Spark Dataframe
  '''
  qwry= df.groupBy(df.columns).count().where(F.col('count') > 1)
  qwry=qwry.withColumn("product_cnt", qwry['count']-1)
  qwry =qwry.where(F.col('product_cnt') >= 1).select(F.sum('product_cnt'))
  return qwry



def percentage_duplicate_rows1(df):
  ''' 
  It computes percentage of duplicate rows.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  qwry= df.groupBy(df.columns).count().where(F.col('count') > 1)
  qwry=qwry.withColumn("duplicate_cnt", qwry['count']-1)
  qwry=qwry.where(F.col('duplicate_cnt') >= 1).select(F.sum('duplicate_cnt'))
  qwry=qwry.withColumn('percentage',(qwry['sum(duplicate_cnt)']/df.count())*100)
  return qwry



def count_distinct_rows1(df):
  return df.distinct().count()




def show_fill_rate1(df):
  ''' 
  It computes fill rate for each column.
  
  Parameters: Dataframe: Spark Dataframe,  
  Return: Pandas Dataframe
  '''
  df=df.select([(((count(when(col(c).isNotNull(), c)).alias(c))/df.count())*100).alias(c) for c in df.columns]).toPandas()
  df=df.T
  df["column_names"]=df.index
  return (df)






def split_date_col1(s,date_column):
  ''' 
  It splits date into individual properties of its own.
  
  Parameters: string,  
  Return: string
  '''
  split_date=F.split(s[date_column], '-')     
  s= s.withColumn('Year', split_date.getItem(0))
  s= s.withColumn('Month', split_date.getItem(1))
  s= s.withColumn('Date_of_month', split_date.getItem(2))
  s=s.withColumn('year_month',F.concat(col('Year'),F.col('month')))
  s=s.withColumn("Week_Day", date_format(s[date_column], "EEE"))
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



def epoch_to_date1(df):
  df=df.withColumn('epoch_clean',F.when(F.col('epoch')>10000000000, (F.col('epoch')/1000).cast("Int")).otherwise(F.col('epoch')))
  df=df.select("*",F.from_unixtime((df.epoch_clean.cast('bigint'))).cast('timestamp').alias('epoch_date'))
  df=df.withColumn('date_only',F.date_format(F.col('epoch_date'),"yyyy-MM-dd HH:mm:ss").cast("date"))
  return df



def data_quality_analysis1(df):
  
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
  #data=spark.createDataFrame(data=lis)
  data=pd.DataFrame(lis, columns=['Total_rows_count', 'Distinct_rows_count', 'Duplicate_rows_counts', 'Percentage_duplicate_row'])
  return data  


def help1():

  x = "These are useful functions that can be used in pyspark environment.\nYou need to import certain dependencies as mentioned in the README file and after that you just install the package using pip install git+gitlab link\nThere are in total 13 functions available here to be used on any spark dataframe\n1.show_missing_values - Returns the dataframe with missing values\n2.count_distinct_values - Returns distinct values of dataframe\n3.count_duplicate_values - Returns count of duplicate values of dataframe\n4.count_duplicate_rows - Returns the count of duplicate rows\n5.percentage_duplicate_rows - Returns the percentage of duplicate rows\n6.count_distinct_rows - Returns the count of distinct rows\n7.show_fill_rate - Shows the fill rate of dataframe\n8.split_date_col - Splitting the date column\n9.epoch_to_date - Converting the epoch to date\n10.data_quality_analysis - Data analysis of entire dataframe\n11.min_max - It will return min and max value of columns specified in the dataframe\n12.replace weekday with numbers - It will replace the weekdays name with values\n13.approx_quantile - Returns the quantile list"
  print(x)


def min_max1(df,rel_cols):
  
  
  if rel_cols is None:
    rel_cols=df.columns
  df_min=df.select([(min(col(c))).alias(c) for c in rel_cols]).toPandas()
  df_min=df_min.T
  df_min["column_names"]=df_min.index
  df_min=df_min.rename(columns={0:'Min'})
  df_max=df.select([(max(col(c))).alias(c) for c in rel_cols]).toPandas()
  df_max=df_max.T
  df_max["column_names"]=df_max.index
  df_max=df_max.rename(columns={0:'Max'})
  df_min_max=pd.merge(df_min,df_max, on="column_names")
  df_min_max=df_min_max[['column_names','Min','Max']]
  return df_min_max


def replace_weekday_with_numbers1(df,col_name):
  df_num = df.replace({"Sun":'0','Mon':'1','Tue':'2','Wed':'3','Thu':'4','Fri':'5','Sat':'6'}, subset=[col_name])
  return df_num


def approx_quantile1(df,col,error=0,quantile_list=None):
  if quantile_list is None:
    return df.approxQuantile(col, [0.0,0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.80,0.90,1.0], error)
  else:
    return df.approxQuantile(col, quantile_list, error)  
