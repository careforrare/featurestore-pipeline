# About this notebook

@author: Yingding Wang

This notebook loads a csv file from the feature store hdfs and save it as a feature group.

**WARNING:**\
You need to select the **Kernel "PySpark"** of Hopsworks Feature Store and restart the Kernel after the selection


```pyspark
import os, sys
from platform import python_version

print(python_version())
```

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th></tr><tr><td>2</td><td>application_1638875459571_0003</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="/hopsworks-api/yarnui/https://resourcemanager.service.consul:8089/proxy/application_1638875459571_0003/">Link</a></td><td><a target="_blank" href="/hopsworks-api/yarnui/https://srvchildre22:8044/node/containerlogs/container_e08_1638875459571_0003_01_000001/dzkj_ml_pipeline__yingding">Link</a></td></tr></table>


    SparkSession available as 'spark'.
    3.7.9


```pyspark
# get the current work dir
CUR_DIR=os.getcwd()
print(f"current dir is {CUR_DIR}")
```


```pyspark
import hsfs
```


```pyspark
# create a connection
connection = hsfs.connection()
# Get the feature store handle for the project's feature store
fs = connection.get_feature_store()
```

    Connected. Call `.close()` to terminate connection gracefully.


```pyspark
# Data Engineering

from hops import hdfs
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDF
```


```pyspark
# covid19 is the subfolder of DataSet
# hdfs_file_path="hdfs:///Projects/{}/covid19/Metadata_clinic.csv".format(hdfs.project_name())
hdfs_file_path=f"hdfs:///Projects/{hdfs.project_name()}/covid19/Metadata_clinic.csv"
meta_clinics_csv: SparkDF = spark.read\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .option("sep", ";")\
    .format("csv")\
    .load(hdfs_file_path)
```


```pyspark
print(type(meta_clinics_csv))
```

    <class 'pyspark.sql.dataframe.DataFrame'>


```pyspark
# spark call show() to display the Spark DataFrame
#meta_clinics_csv.show(5)
```


```pyspark
meta_clinics_fg = fs.create_feature_group(name="covid_clinic_meta_fg", 
                                          version=1, 
                                          primary_key=["Patient code/ Pseudonym"],
                                          description="clinic meta data",
                                          time_travel_format=None,
                                          statistics_config=False
                                         )
```


```pyspark
# save the Spark DataFrame meta_clinics_csv as feature group with name "covid_clinic_meta_fg"
# TODO: feature store doesn't like the column name containing "/" slash, need to remove the invalid sign before save the feature group.
#meta_clinics_fg.save(meta_clinics_csv)
```

## Download the Spark Dataframe to a local Pandas Dataframe using %%sql or %%spark

**WARNING**:\
You shall **NOT** download **large** spark dataframes.

The entire dataframe must fit into the memory of this notebook server. Add the flag `-maxrow x` to limitthe dataframe size.

Reference:\
* Download the Spark DataFrame to local Pandas DataFrame: https://hopsworks.readthedocs.io/en/stable/user_guide/hopsworks/jupyter.html



```pyspark
# uses %%sql magic to select a sql execution on the Spark DataFrame "meta_clinics_csv" 
# and save it to a local Pandas DataFrame "clinic_df"
meta_clinics_csv.createOrReplaceTempView("clinic")
```


```sql
%%sql -c sql -o clinic_df --maxrows 200
SELECT * FROM clinic
```


```pyspark
# Note: not using this instead uses %%sql magic to change the local DataFrame name.

# download from the spark DataFrame with name "meta_clinics_csv" to a local Pandas DataFrame with name "meta_clinics_csv"
#%%spark -o meta_clinics_csv
```


```pyspark
%%local
print(type(clinic_df))
```

    <class 'pandas.core.frame.DataFrame'>



```pyspark
%%local
# now the local pandas DataFrame can be examined
print(f"{type(clinic_df)}")
clinic_df.head(2)
```


```pyspark
# Don't forget to close the connection to hopsworks feature store
connection.close()
```

    Connection closed.


```pyspark

```
