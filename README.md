# EQlocator

*This is a project completed in three week during the [Insight Data Engineering](https://www.insightdataengineering.com/) program in Seattle, Fall 2019. Insight helps recent grads learn the latest big data technologies by building a platform to handle large datasets.*  
[Project slides](https://docs.google.com/presentation/d/1hOo2tHgesBtCbYT8NyO3vpen3G8YBJjjNW8kUfW1218/edit#slide=id.g64690a08f3_0_5)  
[Dash frontend](http://34.217.131.117:8050)

## Project Idea/Business Value
When earthquakes occur their locations are calculated very quickly and crudely in order to issue real-time alerts. More accurate locations can be determined afterwards and used to infer fault locations and make hazard assessments. But how can new location techniques be tested efficiently on a large, historical dataset? I have provided a pipeline to do this using distributed processing, which will allow researchers and policy makers to make more informed hazard determinations.

## Data Source
I used earthquake arrival time data from the Pacific Northwest Seismic Network, [PNSN](https://pnsn.org/). In order to test Spark's functionality I wrote a script to augment the dataset to an arbitrarily large size, in this case ~80 GB.

## Tech Stack  
Amazon AWS S3: This is a common storage option for long-term data of an arbitrarily large size.  
Apache Spark: [Spark](https://spark.apache.org/) is an analytics engine to work with large datasets in a distributed manner. I used the python version, [PySpark](https://spark.apache.org/docs/0.9.0/python-programming-guide.html).  
MySQL: The summary results were stored in a MySQL relational database.  
Dash: Summary results can be accessed and explored by the user through a Dash frontend.

## Engineering Challenge  
Challenge #1: Implementing Pandas user-defined functions (UDFs)
1. Pandas UDFs allow the user to perform transformations on the dataset using Pandas dataframes, which will be returned into the overall Spark dataframes. 
2. In my case I split up the earthquake arrival times for each earthquake and operate on them separately with `df.groupby('Event_id').apply(<pandas_function>)`. `pandas_function` is a separate function that converts that portion of the Spark dataframe into a pandas dataframe using [Apache Arrow](https://arrow.apache.org/docs/python/), operates on it, then returns a new dataframe.
3. This would allow researchers to write and test code using only python/pandas, without worrying about Spark. The tradeoff is that this could take more time than a function implemented entirely within Spark.

Challenge #2: Spark tuning
1. Spark uses multiple nodes to perform distributed computation. The number of nodes, the number of executors, the number of cores per executor, and the RAM per executor, can all be adjusted, among many other parameters.
2. I tested multiple configurations to see how the pipeline could be made more efficient. The difference between the worst and best configurations (with the same total number of cores, worker nodes) was about ~60% in speed.

## Repo Contents
- Dash: Contains the frontend code to display the website that summarizes the new location data.
- Parsing: Contains the scripts to parse the initial dataset into json format and upload it to an S3 bucket.
- Processing: Contains the scripts to read the data from S3, calculate new earthquake locations, and write to a MySQL database, all using Spark.
