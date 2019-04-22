import boto3
import pandas as pd
from io import StringIO
import uuid
import glob, os
from os import path

MAPPER_BUCKET_NAME = 'mapper-data-priyesh'
MAPPER_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/063826520594/MapperQueue.fifo'
CONTINUATOR_FUNC_NAME = 'arn:aws:lambda:us-east-1:063826520594:function:Continutor1'

def dequeue_file_name(sqs):
    response = sqs.receive_message(QueueUrl=MAPPER_QUEUE_URL,
                                   AttributeNames=[
                                       'All'
                                   ],
                                   MaxNumberOfMessages=1,
                                   MessageAttributeNames=[
                                       'All'
                                   ],
                                   VisibilityTimeout=10
                                   # WaitTimeSeconds=5
                                   )
    message_body = ''
    print(response)
    if 'Messages' in response:
        message = response['Messages'][0]
        message_body = message['Body']
        # print(message)
        receipt_handle = message['ReceiptHandle']

        sqs.delete_message(
            QueueUrl=MAPPER_QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
    else:
        message_body = ''

    return message_body

def invoke_continuator(client):
    # client = boto3.client('lambda')
    client.invoke(
        FunctionName=CONTINUATOR_FUNC_NAME,
        InvocationType='Event',
        LogType='None',
        Payload=''
    )
    print("Continuator Invoked!")

def cleanup_temp_space():
    file_list = glob.glob(os.path.join('/tmp/', "*.csv"))
    for file in file_list:
        os.remove(file)

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    sqs = boto3.client('sqs')
    client = boto3.client('lambda')
    cleanup_temp_space()

    has_file_name_from_continuator = False
    src_file_name = event['file_name']
    if src_file_name != '':
        has_file_name_from_continuator = True
    index = 10
    result_df = pd.DataFrame()
    queue_empty = False

    while True:
        if not has_file_name_from_continuator:
            src_file_name = dequeue_file_name(sqs)
        has_file_name_from_continuator = False

        if src_file_name == '':
            queue_empty = True
            print('queue empty')
            break

        local_file_path = '/tmp/' + src_file_name
        s3.Bucket(MAPPER_BUCKET_NAME).download_file(src_file_name, local_file_path)
        df = pd.read_csv(local_file_path)
        result_df = result_df.append(df)
        obj = s3.Object(MAPPER_BUCKET_NAME, src_file_name)
        obj.delete()
        index -= 1
        if index == 0:
            print('index zero')
            break
    if not result_df.empty:
        final_df = result_df.groupby(['Manufacturer']).sum()
        csv_buffer = StringIO()
        final_df.to_csv(csv_buffer)
        file_name = str(uuid.uuid4()) + '.csv'
        # push mapper result to queue
        sqs.send_message(
            QueueUrl=MAPPER_QUEUE_URL,
            MessageAttributes={
                'FileName': {
                    'DataType': 'String',
                    'StringValue': 'Mapper Output'
                }
            },
            MessageBody=file_name,
            MessageGroupId='Mapper'
        )
        # write aggregated result back to mapper bucket
        s3.Object(MAPPER_BUCKET_NAME, file_name).put(Body=csv_buffer.getvalue())
    if not queue_empty:
        invoke_continuator(client)
    else:
        print('Map-reduce complete')
    return

