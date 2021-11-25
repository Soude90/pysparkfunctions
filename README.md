This is a pyspark function file.Basic EDA can be performed using this package on the dataframe and it utilises pyspark function.



**Dependencies** of the package are :


- pyspark sql functions 
- pyspark sql Types
- numpy
- pandas 
- igraph






How to install the package:

`pip install git+https://user_id:private_key@gitlab.com/tata-digital/datascience/ds-knowledgerepo.git@pysparkfunctions`




How to use:

Once you have run the above line,use `import pysparkfunctions`  to use the package

for eg: `pf.count_duplicate_rows(df)` #will return integer after counting the values (import pyspark as pf)


Types of functions in the package:
- 1.show_missing_values
- 2.count_distinct_values
- 3.count_duplicate_values
- 4.count_duplicate_rows
- 5.percentage_duplicate_rows
- 6.count_distinct_rows
- 7.show_fill_rate
- 8.split_date_col
- 9.epoch_to_date
- 10.data_quality_analysis
- 11.min_max
- 12.replace_weekday_with_numbers
- 13.approx_quantile
- 14.help

