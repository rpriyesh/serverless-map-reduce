import boto3
import botocore
import csv
from os import path
from boto3 import client
import glob, os

BUCKET_NAME = 'source-data-priyesh'
OUT_BUCKET_NAME = 'batch-data-priyesh'
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/063826520594/BatchQueue.fifo'

src_file_name = 'split_file'
ext = 'csv'
CHUNK_SIZE = 512000

ENCODING = 'utf-8'


def get_file_name_fact():
    index = 0

    def get_file_name():
        nonlocal index
        f_n = "{0}_{1}.{2}".format(src_file_name, index, ext)
        index += 1
        return f_n

    return get_file_name


get_file_name = get_file_name_fact()


def invoke_continuator():
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:063826520594:function:Continutor1',
        InvocationType='Event',
        LogType='None',
        Payload=''
    )
    print("Continuator Invoked!")


def split_file(s3, sqs, src_file, src_size):
    strt = 0
    eof = False
    with open(src_file, 'rb') as src_strm:
        while True:
            end = CHUNK_SIZE
            abs_pos = src_strm.seek(end, 1)
            scan = 0
            while True:
                c = src_strm.read(1).decode(ENCODING)
                scan += 1
                if (c == '\n') or (c == ''):
                    break
            end += scan
            src_strm.seek(strt)
            chunk = src_strm.read(end)  # .decode(ENCODING)
            strt = abs_pos + scan
            # print(chunk)
            trgt_file = get_file_name()
            # write file to S3 bucket
            file_obj = s3.Object(OUT_BUCKET_NAME, trgt_file)
            file_obj.put(Body=chunk)

            # push file name to SQS Queue
            response = sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageAttributes={
                    'FileName': {
                        'DataType': 'String',
                        'StringValue': trgt_file
                    }
                },
                MessageBody=(trgt_file),
                MessageGroupId=('Batch1')
            )
            print("** == created file ===> {0}".format(trgt_file))
            if strt >= src_size:
                break


def cleanup_temp_space():
    filelist = glob.glob(os.path.join('/tmp/', "*.csv"))
    for f in filelist:
        os.remove(f)


def lambda_handler(event, context):
    print("Splitter invoked.")
    cleanup_temp_space()
    s3 = boto3.resource('s3')
    sqs = boto3.client('sqs')
    conn = client('s3')
    file_name = ''
    tmp_file_name = ''
    for key in conn.list_objects(Bucket=BUCKET_NAME)['Contents']:
        file_name = key['Key']
        tmp_file_name = '/tmp/' + file_name
        print('Begin download - ' + file_name)
        s3.Bucket(BUCKET_NAME).download_file(file_name, tmp_file_name)
        print('File download complete!')
        src_size = path.getsize(tmp_file_name)
        split_file(s3, sqs, tmp_file_name, src_size)
    invoke_continuator()
    return
