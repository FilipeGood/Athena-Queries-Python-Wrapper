import os
import json
from datetime import datetime
import subprocess

import argparse
import json
import pandas as pd    
import time



class AthenaWrapper:
	def execute_query(self, profile, s3_output, workgroup, sql_statement):
		execute_query_cmd = f"aws athena start-query-execution --query-string {sql_statement.rstrip()} --work-group {workgroup} --profile {profile}  --query-execution-context Database=cflogsdatabase,Catalog=AwsDataCatalog  --result-configuration OutputLocation={s3_output}"

		res = subprocess.check_output(execute_query_cmd, shell=True)
		res_json = json.loads(res)
		#print('Query Execution ID: ', res_json['QueryExecutionId'])

		
		query_execution_id = res_json['QueryExecutionId']
		return query_execution_id

	def get_query_status(self, query_execution_id, profile):
		cmd = f"aws athena get-query-execution --query-execution-id {query_execution_id} --profile {profile}"
		res = subprocess.check_output(cmd, shell=True)
		res_json = json.loads(res)

		if res_json['QueryExecution']['Status']['State'] == 'FAILED':
			 return res_json['QueryExecution']['Status']['State'],res_json['QueryExecution']['Status']['StateChangeReason']
		
		return res_json['QueryExecution']['Status']['State'], None

	def get_query_result(self, query_execution_id, profile):

		get_query_res_cmd = f"aws athena get-query-results --query-execution-id {query_execution_id} --profile {profile}"
		res = subprocess.check_output(get_query_res_cmd, shell=True)

		data = json.loads(res)

		columns = list()
		counter = 0
		data_values_list = list()
		for row in data["ResultSet"]["Rows"]:
			aux_list = list()
			d = row['Data']
			for col in d:
				aux_list.append(col['VarCharValue'])

			if counter == 0:
				columns = aux_list
				counter +=1
			else:
				data_values_list.append(aux_list)

		df = pd.DataFrame(data_values_list, columns =columns)
		return df		
