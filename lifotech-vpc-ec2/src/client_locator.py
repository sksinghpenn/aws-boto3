import boto3


class ClientLocator(object):

    def __init__(self, client):
        #Ohio: us-east-2
        self._client = boto3.client(client, region_name='us-east-2')

    def get_client(self):
        return self._client


class EC2Client(ClientLocator):
    def __init__(self):
        super().__init__('ec2')
        

if __name__ == '__main__':
    ec2Client = EC2Client()
    ec2Client = ec2Client.get_client()
    print(ec2Client.__dict__)




