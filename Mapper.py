import boto3
import botocore
import csv
from os import path
import glob, os
import uuid

BUCKET_NAME = 'batch-data-priyesh'
OUT_BUCKET_NAME = 'mapper-data-priyesh'
MAPPER_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/063826520594/MapperQueue.fifo'
BATCH_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/063826520594/BatchQueue.fifo'
CONTINUATOR_FUNC_NAME = 'arn:aws:lambda:us-east-1:063826520594:function:Continutor1'


def dequeue_file_name(sqs):
    response = sqs.receive_message(QueueUrl=BATCH_QUEUE_URL,
                                   AttributeNames=[
                                       'FileName'
                                   ],
                                   MaxNumberOfMessages=1,
                                   MessageAttributeNames=[
                                       'All'
                                   ],
                                   VisibilityTimeout=10
                                   # WaitTimeSeconds=5
                                   )
    message_body = ''
    # print(response)
    if 'Messages' in response:
        message = response['Messages'][0]
        message_body = message['Body']
        # print(message)
        receipt_handle = message['ReceiptHandle']

        sqs.delete_message(
            QueueUrl=BATCH_QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
    else:
        message_body = ''

    return message_body


def invoke_continuator():
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName=CONTINUATOR_FUNC_NAME,
        InvocationType='Event',
        LogType='None',
        Payload=''
    )
    print("Continuator Invoked!")


def cleanup_temp_space():
    filelist = glob.glob(os.path.join('/tmp/', "*.csv"))
    for f in filelist:
        os.remove(f)


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    sqs = boto3.client('sqs')
    cleanup_temp_space()

    has_file_name_from_continuator = False
    src_file_name = event['file_name']
    if src_file_name != '':
        has_file_name_from_continuator = True
    index = 10
    while True:
        if has_file_name_from_continuator == False:
            src_file_name = dequeue_file_name(sqs)
        has_file_name_from_continuator = False
        # src_file_name = message['Body']
        if src_file_name == '':
            print('queue empty')
            break

        local_file_path = '/tmp/' + src_file_name
        s3.Bucket(BUCKET_NAME).download_file(src_file_name, local_file_path)
        with open(local_file_path, newline='') as csvfile:
            filereader = csv.reader(csvfile)
            file_name = str(uuid.uuid4()) + '.csv'
            file_list = []
            file_list.append("Manufacturer,Count \n")
            header_read = False;
            for row in filereader:
                if (header_read is False):
                    header_read = True
                    continue
                make = row[22]
                if (make == 'NULL'):
                    continue
                file_list.append(row[22] + ",1 \n")
            file_obj = s3.Object(OUT_BUCKET_NAME, file_name)
            file_str = ''.join(file_list)
            file_obj.put(Body=file_str)

            # push mapper result to SQS Queue
            response = sqs.send_message(
                QueueUrl=MAPPER_QUEUE_URL,
                MessageAttributes={
                    'FileName': {
                        'DataType': 'String',
                        'StringValue': 'Mapper Output'
                    }
                },
                MessageBody=file_name,
                MessageGroupId=('Mapper')
            )
        index = index - 1
        if index == 0:
            print('index zero')
            break
    invoke_continuator()
    return

