# About this notebook

@author: Yingding Wang

This notebook demonstrate the use of postgres client to connect to a postgres db in core DMZ

* postgres client (psycopg): https://www.psycopg.org/
* Documentation (psycopg): https://www.psycopg.org/docs/usage.html


```python
import sys, os
!{sys.executable} -m pip install --upgrade --user psycopg2-binary python-dotenv
```


```python
import psycopg2
from typing import Dict, List, Tuple
from psycopg2.extensions import cursor
```

## Edit postgres.env file for DB connection


```python
ENV_FILE="scivias_postgres.env"
```

### Uncomment the following cell and edit with your PostresDB credential to connect to DB


```python
'''
%%writefile $ENV_FILE
# environment variables for Postgres DB 14.1 
DB_HOST="Database_Host_DNS_NAME"
DB_NAME="Database_Name"
DB_PORT="Database_Port"
DB_USER="Database_Login_User_Name"
DB_USER_PW="Database_Login_Password"
'''
```


```python
from dotenv import load_dotenv
load_dotenv(dotenv_path=ENV_FILE, override=True)

"""
print(f"\
{os.environ['DB_HOST']}\n\
{os.environ['DB_NAME']}\n\
{os.environ['DB_PORT']}\n\
{os.environ['DB_USER']}\n\
{os.environ['DB_USER_PW']}\n\
")
"""
```


```python
config = {
    "dbname":  os.environ['DB_NAME'],
    "user":    os.environ['DB_USER'],
    "password":os.environ['DB_USER_PW'],
    "host":    os.environ['DB_HOST'],
    "port":    os.environ['DB_PORT']  
}
# print(config)
```


```python
def run_sql(config: Dict, sql_statement: str) -> None: 
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as curs:
            try:
                curs.execute(sql_statement)
            except Exception as cause:
                print(f"{cause}, {type(cause)}")
                
# use a function decorator
def run_sql_cursor(func):
    def inner(config:Dict, sql_statement: str):
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as curs:
                try:
                    # print(type(curs))
                    return func(config, sql_statement, curs)
                except Exception as cause:
                    print(f"{cause}, {type(cause)}")
    return inner                  
```


```python
# create table with name test if not exit
sql1="CREATE TABLE IF NOT EXISTS test (id serial PRIMARY KEY, num integer, data varchar);"
```


```python
run_sql(config, sql1)
```

## Run SQL with return using functional decorator


```python
# https://stackoverflow.com/a/24008869
sql2="select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
```


```python
@run_sql_cursor
def show_all_user_table(config: Dict, sql_statement: str, curs: cursor=None):
    if (curs is not None):
        curs.execute(sql_statement)
        # curs.fetchall() get a list of tuple https://www.psycopg.org/docs/cursor.html#cursor.fetchall
        result: List[Tuple] = curs.fetchall()
        # print list pretty: https://www.geeksforgeeks.org/print-lists-in-python-4-different-ways/    
        for el in result:
            print(*el, sep=",") # print tuple
```


```python
show_all_user_table(config, sql2)
```


```python

```
