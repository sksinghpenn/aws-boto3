from src.ec2.vpc import VPC
from src.client_locator import EC2Client
from src.ec2.ec2 import EC2


def main():
    # Create a VPC
    ec2_client = EC2Client().get_client()
    vpc = VPC(ec2_client)

    vpc_response = vpc.create_vpc()

    print('VPC created' + str(vpc_response))

    # Add name ag to VPC
    vpc_name = 'Boto3-VPC'
    vpc_id = vpc_response['Vpc']['VpcId']
    vpc.add_name_tag(vpc_id, vpc_name)

    print('added ' + vpc_name + ' to ' + vpc_id)

    # Create an IGW
    igw_response = vpc.create_internet_gateway()

    igw_id = igw_response['InternetGateway']['InternetGatewayId']

    vpc.attach_igw_to_vpc(vpc_id, igw_id)

    print('VPC ' + vpc_id + 'is attached to the internet gateway ' + igw_id)

    # Create a public subnet
    public_subnet_response = vpc.create_subnet(vpc_id, '10.0.1.0/24')
    print('Subnet created for VPC ' + vpc_id + ': ' + str(public_subnet_response))

    # Create a public route table
    public_route_table_response = vpc.create_public_route_table(vpc_id)

    print("public_route_table_response " + str(public_route_table_response))
    rtb_id = public_route_table_response['RouteTable']['RouteTableId']

    # Adding IGW to the public route table
    vpc.create_igw_route_to_route_table(rtb_id, igw_id)

    public_subnet_id = public_subnet_response['Subnet']['SubnetId']

    # Associate subnet with the route table
    vpc.associate_subnet_with_route_table(subnet_id=public_subnet_id, rtb_id=rtb_id)

    # Allow auto-assign public ip addresses for subnet
    vpc.allow_auto_assign_ip_addresses_for_subnet(subnet_id=public_subnet_id)

    # Create private subnet
    private_subnet_response = vpc.create_subnet(vpc_id, cidr_block='10.0.2.0/24')
    private_subnet_id = private_subnet_response['Subnet']['SubnetId']

    print('Created private subnet ' + private_subnet_id + ' for VPC ' + vpc_id)

    # Add name tag to the private subnet
    vpc.add_name_tag(private_subnet_id, 'Boto3-Private-Subnet')

    # Add name tag to the public subnet
    vpc.add_name_tag(public_subnet_id, 'Boto3-Public-Subnet')

    # Create EC2 instances
    ec2 = EC2(ec2_client)

    # Create EC2 key pair
    key_pair_name = 'Boto3_EC2_Key_Pair'
    key_pair_response = ec2.create_key_pair(key_pair_name)

    print('Created key pair with the name: ' + key_pair_name + ' and the response is ' + str(key_pair_response))

    # Create Security Group
    public_security_group_name = 'Boto3-Public-SG'
    public_security_group_description = 'public security  group for  the public subnet'
    public_security_group_response = ec2.create_security_group(public_security_group_name,
                                                               public_security_group_description,
                                                               vpc_id)
    print('Created security group ' + public_security_group_name + ' with response ' + str(
        public_security_group_response))

    # Add rule to the security group
    public_security_group_id = public_security_group_response['GroupId']
    add_inbound_rule_response = ec2.add_inbound_rule_to_security_group(public_security_group_id)

    print('Added public access rule to the security group ' + public_security_group_name)

    user_data = """#!/bin/bash
                yum update -y
                yum install httpd -y
                service httpd start
                chkconfig httpd on
                echo "<html><body>Welcome to boto3</body></html>" > /var/www/html/index.html """

    image_id = 'ami-02bcbb802e03574ba'
    launch_ec2_instance_response_public_subnet_response = ec2.launch_ec2_instance(
        image_id=image_id,
        min_count=1,
        max_count=1,
        key_name=key_pair_name,
        subnet_id=public_subnet_id,
        security_group_id=public_security_group_id,
        user_data=user_data)

    print('Launching instance in public subnet with AMI ' + image_id + ' and instanceId' +
          launch_ec2_instance_response_public_subnet_response['Instances'][0]['InstanceId'])

    private_security_group_name = 'Boto3-Private-SG'
    private_security_group_description = 'Private Security Group for Private Subnet'
    private_security_group_response = ec2.create_security_group(private_security_group_name,
                                                                private_security_group_description, vpc_id)

    private_security_group_id = private_security_group_response['GroupId']

    ec2.add_inbound_rule_to_security_group(private_security_group_id)

    # Launch a private EC2 instance
    launch_ec2_instance_response_in_private_subnet_response = ec2.launch_ec2_instance(
        image_id=image_id,
        min_count=1,
        max_count=1,
        key_name=key_pair_name,
        subnet_id=private_subnet_id,
        security_group_id=private_security_group_id,
        user_data=user_data)

    print('Launching instance in private subnet with AMI ' + image_id + ' and instanceId' +
          launch_ec2_instance_response_in_private_subnet_response['Instances'][0]['InstanceId'])


def describe_instances():
    ec2_client = EC2Client().get_client()
    ec2 = EC2(ec2_client)
    ec2_response = ec2.describe_ec2_instances()
    print(str(ec2_response))


def modify_instance():
    instance_id = 'i-012a14fdc79b7a599'

    ec2_client = EC2Client().get_client()
    ec2 = EC2(ec2_client)
    ec2.modify_ec2_instance(instance_id)
    

def stop_instance():
    instance_id = 'i-012a14fdc79b7a599'

    ec2_client = EC2Client().get_client()
    ec2 = EC2(ec2_client)
    ec2.stop_ec2_instance(instance_id)


def start_instance():
    instance_id = 'i-012a14fdc79b7a599'

    ec2_client = EC2Client().get_client()
    ec2 = EC2(ec2_client)
    ec2.start_ec2_instance(instance_id)


def terminate_instance():
    instance_id = 'i-012a14fdc79b7a599'

    ec2_client = EC2Client().get_client()
    ec2 = EC2(ec2_client)
    ec2.terminate_ec2_instance(instance_id)



if __name__ == '__main__':
    # main()
    # describe_instances()
    #modify_instance()
    #stop_instance()
    #start_instance()
    terminate_instance()
