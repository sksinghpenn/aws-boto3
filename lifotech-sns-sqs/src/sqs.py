import boto3
import json

QUEUE_NAME = 'MyTest-SQS-Queue'
QUEUE_NAME_URL = 'https://us-east-2.queue.amazonaws.com/066607821278/MyTest-SQS-Queue'
FIFO_QUEUE_NAME = 'MyTestQueue.fifo'
FIFO_QUEUE_NAME_URL = 'https://us-east-2.queue.amazonaws.com/066607821278/MyTestQueue.fifo'
DEAD_LETTER_QUEUE_NAME ='Dead-Letter-Queue-for-Main'
MAIN_QUEUE = 'Main-Queue'
MAIN_QUEUE_URL = 'https://us-east-2.queue.amazonaws.com/066607821278/Main-Queue'


def sqs_client():
    sqs = boto3.client('sqs', region_name='us-east-2')

    """ :type : pyboto3.sqs """

    return sqs


def create_sqs_queue():
    """
    In AWS, there is a chance of message getting lost in general queue.
    But FIFO queue, there is guarantee of the message delivery.
    """

    return sqs_client().create_queue(
        QueueName=QUEUE_NAME
    )


def create_fifo_queue():
    return sqs_client().create_queue(
        QueueName=FIFO_QUEUE_NAME,
        Attributes={
            'FifoQueue': 'True'
        }
    )


def create_queue_for_dead_letter():
    return sqs_client().create_queue(
        QueueName=DEAD_LETTER_QUEUE_NAME
    )


def create_main_queue_for_dead_letter():
    redrive_policy = {
        'deadLetterTargetArn': 'arn:aws:sqs:us-east-2:066607821278:Dead-Letter-Queue-for-Main',
        'maxReceiveCount': 3
    }


    return sqs_client().create_queue(
        QueueName=MAIN_QUEUE,
        Attributes={
            'DelaySeconds': '0',
            'MaximumMessageSize': '262144',
            'VisibilityTimeout': '30',
            'MessageRetentionPeriod': '345680',
            'ReceiveMessageWaitTimeSeconds': '0',
            'RedrivePolicy': json.dumps(redrive_policy)

        }
    )


def find_queue(prefix):

    return sqs_client().list_queues(
        QueueNamePrefix=prefix
    )


def list_all_queues():
    return sqs_client().list_queues()


def get_queue_attributes():
    return sqs_client().get_queue_attributes(
        QueueUrl=MAIN_QUEUE_URL,
        AttributeNames=['All']
    )


def update_queue_attributes():

    return sqs_client().set_queue_attributes(
       QueueUrl=MAIN_QUEUE_URL,
       Attributes={
           'MaximumMessageSize': '131072',
           'VisibilityTimeout': '15'
       }
    )


def delete_queue(queue_url):
    return sqs_client().delete_queue(QueueUrl=queue_url)


def send_message_to_queue():
    return sqs_client().send_message(
        QueueUrl=MAIN_QUEUE_URL,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'My Message Title'
             },
            'Author': {
                'DataType': 'String',
                'StringValue': 'Shashi Singh'
            },
            'Time': {
                'DataType': 'Number',
                'StringValue': '6'
            }
        },
        MessageBody='This is my first SQQ Message'
        
    )


def send_batch_messages_to_queue():
    return sqs_client().send_message_batch(
        QueueUrl=MAIN_QUEUE_URL,
        Entries=[
            {
                'Id': 'FirstMessage',
                'MessageBody': 'This is the 1st message of batch'
            },
            {
                'Id': 'SecondMessage',
                'MessageBody': 'This is the 2nd message of batch'
            },
            {
                'Id': 'ThirdMessage',
                'MessageBody': 'This is the 3rd message of batch'
            },
            {
                'Id': 'ForthMessage',
                'MessageBody': 'This is the 4th message of batch'
            }

        ]

    )


def poll_queue_for_messages():
    return sqs_client().receive_message(
        QueueUrl=MAIN_QUEUE_URL,
        MaxNumberOfMessages=10
    )


def process_message_from_queue():
    queued_messages = poll_queue_for_messages()
    if 'Messages' in queued_messages and len(queued_messages['Messages']) >= 1:
        for message in queued_messages['Messages']:
            print("Processing message " + message['MessageId'] + "with test" + message['Body'])
            delete_message_from_queue(message['ReceiptHandle'])


def process_message_from_queue_to_change_visibility_timeout():
    queued_messages = poll_queue_for_messages()
    if 'Messages' in queued_messages and len(queued_messages['Messages']) >= 1:
        for message in queued_messages['Messages']:
            print("Processing message " + message['MessageId'] + "with test" + message['Body'])
            change_message_visibility_timeout(message['ReceiptHandle'])
            

def delete_message_from_queue(receipt_handle):
    sqs_client().delete_message(
        QueueUrl=MAIN_QUEUE_URL,
        ReceiptHandle=receipt_handle
    )
    

def change_message_visibility_timeout(receipt_handle):
    return sqs_client().change_message_visibility(
        QueueUrl=MAIN_QUEUE_URL,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=5
    )


def purge_queue():
    return sqs_client().purge_queue(QueueUrl=MAIN_QUEUE_URL)


if __name__ == '__main__':

    #print(create_sqs_queue())
    #print(create_fifo_queue())
    #print(create_queue_for_dead_letter())
    #print(create_main_queue_for_dead_letter())

    #print(find_queue('MyTest'))
    #print(list_all_queues())
    #print(get_queue_attributes())
    #print(update_queue_attributes())
    #print(delete_queue(QUEUE_NAME_URL))
    
    #print(send_message_to_queue())

    #print(send_batch_messages_to_queue())

    #print(poll_queue_for_messages())

    #print(process_message_from_queue())

    #print(process_message_from_queue_to_change_visibility_timeout())



    print(purge_queue())

