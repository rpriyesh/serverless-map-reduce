import boto3
from boto3 import client
import json

MAPPER_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/063826520594/MapperQueue.fifo'
BATCH_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/063826520594/BatchQueue.fifo'
MAPPER_FUNC_NAME = 'arn:aws:lambda:us-east-1:063826520594:function:Mapper2'
REDUCER_FUNC_NAME = 'arn:aws:lambda:us-east-1:063826520594:function:Reducer'


def dequeue_file_name(sqs, queue_name):
    response = sqs.receive_message(QueueUrl=queue_name,
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
        print('Batch queue not empty!')
        message = response['Messages'][0]
        message_body = message['Body']
        # print(message)
        receipt_handle = message['ReceiptHandle']

        sqs.delete_message(
            QueueUrl=queue_name,
            ReceiptHandle=receipt_handle
        )
    else:
        message_body = ''
    return message_body


def invoke_next_action(func_name, file_name):
    client = boto3.client('lambda')
    payload = {}
    payload['file_name'] = file_name
    # print(json.dumps(payload))
    response = client.invoke(
        FunctionName=func_name,
        InvocationType='Event',
        LogType='None',
        Payload=json.dumps(payload)
    )
    print("Function invoked!")


def lambda_handler(event, context):
    sqs = boto3.client('sqs')

    def iterations():
        mapper_file = dequeue_file_name(sqs, BATCH_QUEUE_URL)
        # print('mapper file name: ' + mapper_file)
        if mapper_file != '':
            invoke_next_action(MAPPER_FUNC_NAME, mapper_file)
        else:
            # print ("Batch queue empty!")
            reducer_file = dequeue_file_name(sqs, MAPPER_QUEUE_URL)
            # print('reducer file name: ' + reducer_file)
            if reducer_file != '':
                invoke_next_action(REDUCER_FUNC_NAME, reducer_file)
            else:
                print("Mapper queue empty!")
                return

    iterations()
    iterations()
    return

