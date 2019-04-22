import boto3
import botocore
import csv
from os import path
import uuid

BUCKET_NAME = 'source-data-priyesh'
OUT_BUCKET_NAME = 'batch-data-priyesh'
KEY = '2015_Make_Model.csv'

SOURCE_FILE = '/tmp/2015_Make_Model.csv'
src_file_name = '2015_Make_Model'
ext = 'csv'

def lambda_handler(event, context):
	print('Splitter using csv.reader begin!')
	s3 = boto3.resource('s3')
	s3.Bucket(BUCKET_NAME).download_file(KEY, SOURCE_FILE)
	print('File download complete!')

	file_list = []
	counter = 0
	file_name = ''
	with open('/tmp/2015_Make_Model.csv', newline='') as csvfile:
		rows = csv.reader(csvfile)
		for row in rows:
			counter += 1
			# print(''.join(row) + )
			file_list.append(','.join(row) + '\n')
			if counter % 5000 == 0:
				# print(counter)
				file_name = str(uuid.uuid4()) + '.csv'
				file_obj = s3.Object(OUT_BUCKET_NAME, file_name)
				file_content = ''.join(file_list)
				file_obj.put(Body=file_content)
				file_list = []
	print('Splitter using csv.reader complete!')
	return