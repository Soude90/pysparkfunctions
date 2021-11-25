from pysparkfunctions import utils

__version__ = '0.10.3'

def show_missing_values(df):
    '''Returns missing values

       Parameters: Dataframe: Spark Dataframe,  
       Return: Pandas Dataframe
    '''
    return utils.show_missing_values1(df)

def count_distinct_values(df):
    '''Returns distinct values of dataframe

       Parameters: Dataframe: Spark Dataframe,  
       Return: Pandas Dataframe
    ''' 
    return utils.count_distinct_values1(df)

def count_duplicate_values(df):
    '''Returns no of duplucate entries

       Parameters: Dataframe: Spark Dataframe,  
       Return: Pandas Dataframe
    '''
    return utils.count_duplicate_values1(df) 

def count_duplicate_rows(df):
    '''Returns number of duplicate rows

       Parameters: Dataframe: Spark Dataframe,  
       Return: Pandas Dataframe
    '''
    return utils.count_duplicate_rows1(df)

def percentage_duplicate_rows(df):
    '''Returns the percentage of duplicate rows

       Parameters: Dataframe: Spark Dataframe,  
       Return: Spark Dataframe
    '''
    return utils.percentage_duplicate_rows1(df)

def count_distinct_rows(df):
    '''Returns the count of distinct rows
    
       Parameters: Dataframe: Spark Dataframe,  
       Return: Pandas Dataframe
    '''
    return utils.count_distinct_rows1(df)

def show_fill_rate(df):
    '''Shows the fill rate of dataframe
       Parameters: Dataframe: Spark Dataframe,  
       Return: Pandas Dataframe
    '''
    return utils.show_fill_rate1(df)

def distinct_values_each_column(df):
    '''Returns the number of distinct values of each column
    
       Parameters: Dataframe: Spark Dataframe,  
       Return: Spark Dataframe
    '''
    return utils.distinct_values_each_column1(df)

def split_date_col(s,date_column):
    '''Splitting the date column
    
       Parameters: Datframe,String,  
       Return: Spark dataframe
    '''
    return utils.split_date_col1(s,date_column)

def epoch_to_date(df):
    '''Converting the epoch to date
    
       Parameters: Dataframe: Spark Dataframe,  
       Return: Spark Dataframe
    '''
    return utils.epoch_to_date1(df)


def data_quality_analysis(df):
    '''Data analysis of entire dataframe
    
       Parameters: Dataframe: Spark Dataframe,  
       Return: Spark Dataframe
    '''
    return utils.data_quality_analysis1(df)


def help():
   '''info about package'''

   utils.help1()
   return None


def min_max(df,rel_cols):
   '''It will return min and max value of columns specified in the dataframe
   
      Parameter: Dataframe:Spark Dataframe
      Return:Pandas Dataframe
   '''

   return utils.min_max1(df,rel_cols)


def replace_weekday_with_numbers(df,col_name):
   '''It will replace the weekdays name with values 
      
      Parameters:Dataframe:Spark Dataframe,Column containg weekdays
      Return:Spark Dataframe
   '''

   return utils.replace_weekday_with_numbers1(df,col_name)


def approx_quantile(df,col,error,quantile_list):
   ''' Returns the quantile list 
   
       Parameters:Dataframe:Spark Dataframe,column name,error value,quantile list
       Return:List
   '''

   return utils.approx_quantile1(df,col,error,quantile_list)






