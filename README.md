
# How to add columns to Polars dataframes fast

The code and artifacts in this repository document speed comparisons of two different methods for dynamically adding a variable number of columns to a Polars dataframe.  In this case, the columns calculate moving averages of a provided column of data, though the methods should apply more generally beyond calculating moving averages.  The two methods are:

1. Calculating each column sequentially one-by-one in a loop, then combining the columns into a dataframe
2. Adding the command to calculate each column to a query string, then executing the entire query at one time

The second method takes advantage of the Polars library's ability to parallelize its workloads and is shown in testing to be faster.  A detailed description and results are available at the [notebook in this repository](https://github.com/afairless/polars_add_columns_dynamically_fast/blob/main/notebook/moving_average_timing.ipynb) or at the [webpage](https://afairless.com/add-columns-to-polars-dataframes-quickly/).
