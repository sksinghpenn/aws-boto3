import boto3

rds_client = boto3.client('rds')
""" :type : pyboto3.rds"""

RDS_DB_SUBNET_GROUP = 'my-rds-db-subnet-group'


def create_db_subnet_group():
    print('Creating RDS DB Subnet Group : ' + RDS_DB_SUBNET_GROUP)
    rds_client.create_db_subnet_group(
        DBSubnetGroupName=RDS_DB_SUBNET_GROUP,
        DBSubnetGroupDescription='My own db subnet group',
        SubnetIds=['subnet-03ca09378a6de089b', 'subnet-0703139daf3e8a7bf', 'subnet-0fd83d27a2cd5f3a1']
    )

def create_db_security_group():
    print('Creating DB security group and add inbound rule')
    ec2 = boto3.client('ec2')

    """ :type : pyboto3.ec2 """

    # create security group
    security_group = ec2.create_security_group(
        GroupName='My-RDS-public-sg',
        Description='My RDS public security group',
        VpcId='vpc-2f180647'
    )

    # get id of the security group
    security_group_id = security_group['GroupId']
    print('Created RDS Security Group with id  ' + security_group_id)
    return  security_group_id


def add_inbound_rule(security_group_id):

    ec2 = boto3.client('ec2')
    # add public access rule to sg
    ec2.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': 5432,
                'ToPort': 5432,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }
        ]
    )

    print("Added inbound access rule to security group with id " + security_group_id)


def launch_rds_instance():
    create_db_subnet_group()
    security_group_id = create_db_security_group()
    add_inbound_rule(security_group_id)

    rds_client.create_db_instance(
        DBName='myPgBoto3',
        DBInstanceIdentifier='myPgBoto3',
        DBInstanceClass='db.t2.micro',
        Engine='postgres',
        MasterUsername='postgres',
        MasterUserPassword='Sangam006',
        AllocatedStorage=20,
        MultiAZ=False,
        StorageType='gp2',
        PubliclyAccessible=True,
        DBSubnetGroupName=RDS_DB_SUBNET_GROUP,
        VpcSecurityGroupIds=[security_group_id]

    )


if __name__ == '__main__':
    launch_rds_instance()



