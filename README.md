# Athena-Queries-Python-Wrapper

Very simple python script that let's you run athena sql queries from python. The script enables you to execute queries and get the result in a pandas dataframe

The class has 3 methods:
*  execute_query
*  get_query_status
*  get_query_result

You will need the following arguments from AWS:
* profile
* s3_output_path
* workgroup (primary)

Example:
```python
from athena_wrapper import AthenaWrapper
import pandas as pd 

f = open('test_query.txt', 'r')
sql_statement = f.read()
sql_statement = sql_statement.replace("###REPLACE_ME###", <database_plus_table_names>)
f.close()


athena = AthenaWrapper()
query_execution_id = athena.execute_query(profile, s3_output, workgroup, sql_statement)

status, error_msg = athena.get_query_status(query_execution_id, profile)
while status == 'RUNNING':
  # Query still executing
	time.sleep(3)
	status, error_msg= athena.get_query_status(query_execution_id, profile)

if status == 'FAILED':
	 print(f'Query {query_execution_id} Failed! {error_msg}')
  return None
	
df = athena.get_query_result(query_execution_id, profile)
```
