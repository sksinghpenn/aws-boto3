class EC2:

    def __init__(self, client):
        self.__client = client
        """ :type : pyboto3.ec2 """

    def create_key_pair(self, key_name):
        print('Creating key pair with key name ' + key_name)
        return self.__client.create_key_pair(KeyName=key_name)

    def create_security_group(self, group_name, description, vpc_id):
        print('Creating security group with group name' + group_name + ' and description '
              + description + ' and vpc id ' + vpc_id)
        return self.__client.create_security_group(GroupName=group_name, Description=description, VpcId=vpc_id)

    def add_inbound_rule_to_security_group(self, security_group_id):
        self.__client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'FromPort': 80,
                    'IpProtocol': 'tcp',
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}],
                    'ToPort': 80
                }, {
                    'FromPort': 22,
                    'IpProtocol': 'tcp',
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}],
                    'ToPort': 22
                }
            ]

        )

    def launch_ec2_instance(self, image_id, min_count, max_count, key_name,
                            subnet_id, security_group_id, user_data):

        print('Launching instance with ' + str(min_count) + ' in subnet id ' + subnet_id)

        return self.__client.run_instances(
            ImageId=image_id,
            MinCount=min_count,
            MaxCount=max_count,
            KeyName=key_name,
            SubnetId=subnet_id,
            InstanceType='t2.micro',
            SecurityGroupIds=[security_group_id],
            UserData=user_data)

    def describe_ec2_instances(self):
        print("Describing EC2 instances.....")
        return self.__client.describe_instances()

    def modify_ec2_instance(self, instance_id):
        print('Modifying EC2 instance ' + instance_id)
        self.__client.modify_instance_attribute(
            InstanceId=instance_id,
            DisableApiTermination={'Value': True},
        )

    def stop_ec2_instance(self,instance_id):
        print('Stopping EC2 instance ' + instance_id)
        self.__client.stop_instances(InstanceIds=[instance_id])

    def start_ec2_instance(self, instance_id):
        print('Starting EC2 instance ' + instance_id)
        self.__client.start_instances(
            InstanceIds=[instance_id]
        )

    def terminate_ec2_instance(self, instance_id):
        print('Terminating EC2 instance ' + instance_id)
        self.__client.terminate_instances(
            InstanceIds=[instance_id]
        )


