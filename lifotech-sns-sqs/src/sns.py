import boto3


TOPIC_NAME = 'SubscriptionTopic'
TOPIC_NAME_ARN = "arn:aws:sns:us-east-1:066607821278:SubscriptionTopic"


def get_sns_client():

    sns_client = boto3.client('sns', region_name = 'us-east-1')

    """ :type: pyboto3.sns"""
    return sns_client


def create_topic():
    return get_sns_client().create_topic(Name=TOPIC_NAME)


def get_topics():
    return get_sns_client().list_topics()


def get_topic_attribues(topic_arn):
    return get_sns_client().get_topic_attributes(TopicArn=topic_arn)


def update_topic_attributes(topic_arn):
    return get_sns_client().set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName='DisplayName',
        AttributeValue=TOPIC_NAME + '-Updated'
    )


def delete_topic(topic_arn):
    return get_sns_client().delete_topic(TopicArn=topic_arn)


def create_email_subscription(topic_arn, email_id):
    return get_sns_client().subscribe(TopicArn=topic_arn, Protocol='email', Endpoint=email_id)


def create_sms_subscription(topic_arn, phone_number):
    return get_sns_client().subscribe(TopicArn=topic_arn, Protocol='sms', Endpoint=phone_number)


def create_sqs_subscription(topic_arn, queue_arn):
    return get_sns_client().subscribe(TopicArn=topic_arn, Protocol='sqs', Endpoint=queue_arn)


def create_queue(queue_name):
    sqs_client = boto3.client("sqs", region_name='us-east-1')

    """ :type :pyboto3.sqs"""
    return sqs_client.create_queue(QueueName=queue_name)


def publish_message_to_subscribers(topic_arn):
    get_sns_client().publish(
        TopicArn=topic_arn,
        Message='You ae receiving it because you subscribed')



if __name__ == '__main__':
    #print(create_topic())

    #print(get_topics())

    #print(get_topic_attribues(TOPIC_NAME_ARN))

    #rint(update_topic_attributes(TOPIC_NAME_ARN))

    #print(delete_topic(TOPIC_NAME_ARN))




    MAIN_QUEUE_NAME = 'MyTestQueue'
    MAIN_QUEUE_ARN =  'arn:aws:sqs:us-east-1:066607821278:MyTestQueue'
    TOPIC_NAME_ARN_EAST_REGION_1='arn:aws:sns:us-east-1:066607821278:SubscriptionTopic'

    #print(create_queue(MAIN_QUEUE_NAME))

    #print(create_sqs_subscription(TOPIC_NAME_ARN_EAST_REGION_1, MAIN_QUEUE_ARN))

    #print(create_email_subscription(TOPIC_NAME_ARN_EAST_REGION_1, 'rod.jhonson@gmail.com'))
    #print(create_sms_subscription(topic_arn=TOPIC_NAME_ARN_EAST_REGION_1, phone_number='+12157915136'))

    print(publish_message_to_subscribers(TOPIC_NAME_ARN_EAST_REGION_1))
