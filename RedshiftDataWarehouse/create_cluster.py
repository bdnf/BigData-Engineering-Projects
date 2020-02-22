import os
import time
import pandas as pd
import boto3
import json
import psycopg2

import configparser

# time to wait to extract endpoint from the Redshift cluster after creation
WAITING_TIME = 0
MAX_WAITING_TIME = 180 # 3 minutes

def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    '''
    Create IAM Role for Redshift that allows it to use AWS services
    '''

    try:
        print("Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)


    print("Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("Getting the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    return roleArn


def create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD):
    '''
    Creates Redshift cluster
    '''

    try:
        response = redshift.create_cluster(
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print("Error occured %s" % e)


def get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    global  WAITING_TIME, MAX_WAITING_TIME
    '''
    Retrieve metadata of Redshift cluster
    '''

    def prettyRedshiftProps(props):
        print("Props are: ", props )
        # older pandas version may raise an error
        try:
            pd.set_option('display.max_colwidth', None)
        except ValueError as e:
            print("And error during creating Pandas df occured. Fixing the issue")
            pd.set_option('display.max_colwidth', -1)

        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    props = prettyRedshiftProps(myClusterProps)

    print("Cluster props are: \n", props)

    roleArn = ""
    endpoint = myClusterProps.get('Endpoint', None)

    seconds = 10
    while (endpoint is None):
        if WAITING_TIME >= MAX_WAITING_TIME:
            break

        # cluster is not ready yet, let's wait a bit
        time.sleep(seconds)
        WAITING_TIME += 15
        print("Cluster is not yet ready, let check again if endpoint is accessible (Time passed: %s seconds) " % WAITING_TIME)
        if endpoint is not None:
            break
        else:
            myClusterProps, endpoint, roleArn = get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)

    endpoint = endpoint.get('Address', None)
    roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", endpoint)
    print("DWH_ROLE_ARN :: ", roleArn)

    return myClusterProps, endpoint, roleArn


def open_ports(ec2, myClusterProps, DWH_PORT, cidr_range):
    '''
    Update security group to allow access via TCP redshift port
    '''
    print("Updating security group")
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp=cidr_range,
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def delete_cluster(redshift, iam, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME):
    """
        [Optional] Cleanup and Cluster delete, WITHOUT! creating snapshot
    """
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)


def main():

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    # get credentals from ENV variables
    KEY                    = os.getenv("REDSHIFT_IAM_KEY")
    SECRET                 = os.getenv("REDSHIFT_IAM_SECRET")

    # get all info from Config file
    DWH_CLUSTER_TYPE       = config.get("CLUSTER","CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER","NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER","NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DB","DB_NAME")
    DWH_DB_USER            = config.get("DB","DB_USER")
    DWH_DB_PASSWORD        = config.get("DB","DB_PASSWORD")
    DWH_PORT               = config.get("DB","DB_PORT")

    DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

    # IP range to access cluster. use 0.0.0.0/0 by default and port DWH_PORT
    MY_CIDR_IP_RANGE       = config.get("SECURITY_GROUP", "ALLOWED_CIDR_IP")

    AWS_REGION             = config.get("CLUSTER","REGION")

    ec2 = boto3.resource('ec2',
                        region_name=AWS_REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name=AWS_REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name=AWS_REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

    create_cluster(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD)

    myClusterProps, redshiftEndpoint, _ = get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)

    print("Cluster Endpoint is:", redshiftEndpoint)

    # Setting new redshift endpoint
    config.set("DB","HOST", redshiftEndpoint)
    config.set("IAM_ROLE", "ARN", roleArn)

    # dynamically update config file
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)

    open_ports(ec2, myClusterProps, DWH_PORT, MY_CIDR_IP_RANGE)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    print('Connected')

    conn.close()

    # !!Uncomment next line after you finished to delete the cluster
    #delete_cluster(redshift, iam, DWH_CLUSTER_IDENTIFIER, DWH_IAM_ROLE_NAME)


if __name__ == "__main__":
    main()
