import boto3
import json
import sys,os
import pprint

metric_info=[
    {
        "operation": "read",
        "table_scaleable_dimension": "dynamodb:table:ReadCapacityUnits",
        "index_scaleable_dimension": "dynamodb:index:ReadCapacityUnits",
        "metric_type": "DynamoDBReadCapacityUtilization"
    },
    {
        "operation": "write",
        "table_scaleable_dimension": "dynamodb:table:WriteCapacityUnits",
        "index_scaleable_dimension": "dynamodb:index:WriteCapacityUnits",
        "metric_type": "DynamoDBWriteCapacityUtilization"
    }
]

def get_resource_policy_name(resource_id,metric_type):
    policy_name=metric_type + ":" + resource_id
    return policy_name

def get_role_arn(role_name):
    role_arn=None
    response=None
    iam_client = boto3.client('iam')

    try:
        response = iam_client.get_role(
            RoleName=role_name
        )
        role_arn=response['Role']['Arn']
    except Exception, e:
        print "Failed to get role ARN " + str(e)

    return role_arn

def scalable_target_exists(resource_id,scalable_dimension):
    exists=False
    response=None

    print "Checking if scalable target exists for " + resource_id + " for dimension " + scalable_dimension
    client = boto3.client('application-autoscaling')

    try:
        response = client.describe_scalable_targets(
            ServiceNamespace='dynamodb',
            ResourceIds=[
                resource_id,
            ],
            ScalableDimension=scalable_dimension
        )
    except Exception, e:
        print "Failed to describe scalable targets " + str(e)

    if response:
        if response['ScalableTargets']:
            exists=True

    return exists

def register_scalable_target(resource_id,scalable_dimension,role_arn,min_tput,max_tput):
    status=False
    response=None

    print "Registering scalable target for " + resource_id + " for dimension " + scalable_dimension
    client = boto3.client('application-autoscaling')

    try:
        response = client.register_scalable_target(
            ServiceNamespace='dynamodb',
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
            MinCapacity=int(min_tput),
            MaxCapacity=int(max_tput),
            RoleARN=role_arn
        )
    except Exception, e:
        print "Failed to register scalable target " + str(e)

    if response:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            status=True

    return status

def scaling_policy_exists(resource_id,scalable_dimension,metric_type):
    exists=False
    response=None
    policy_name=get_resource_policy_name(resource_id,metric_type)

    print "Checking if scaling policy exists with name  " + policy_name

    if policy_name:
        client = boto3.client('application-autoscaling')

        try:
            response = client.describe_scaling_policies(
                PolicyNames=[
                    policy_name,
                ],
                ServiceNamespace='dynamodb',
                ResourceId=resource_id,
                ScalableDimension=scalable_dimension
            )
        except Exception, e:
            print "Failed to describe scaling policies " + str(e)

        if response:
            if response['ScalingPolicies']:
                exists=True

    return exists

def put_scaling_policy(resource_id,metric_type,scalable_dimension):
    status=False
    response=None
    policy_name=get_resource_policy_name(resource_id,metric_type)

    print "Putting scaling policy for " + resource_id + " for dimension " + scalable_dimension + " metric " + metric_type
    client = boto3.client('application-autoscaling')

    try:
        response = client.put_scaling_policy(
            PolicyName=policy_name,
            ServiceNamespace='dynamodb',
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
            PolicyType='TargetTrackingScaling',
            TargetTrackingScalingPolicyConfiguration={
                'TargetValue': 50,
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': metric_type
                }
            }
        )

    except Exception, e:
        print "Failed to put scaling policy " + str(e)

    if response:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            status=True

    return status

# resource type is one of index or table
def handle_resource(resource_id,resource_type):
    status=True
    dimension=None

    rolename=os.environ['rolename']
    max_tput=os.environ['max_tput']
    min_tput=os.environ['min_tput']

    role_arn=get_role_arn(rolename)
    print "Role ARN is %s" % role_arn

    if role_arn:
        print "Role " + rolename + " found in IAM"

        # are we dealing with an index or table scalable dimension
        if resource_type == 'index':
            dimension='index_scaleable_dimension'
        elif resource_type == 'table':
            dimension='table_scaleable_dimension'
        else:
            print "Unknown resource type"
            status=False

        if status:
            # Now see if the items we need exist and create if not
            for metric in metric_info:
                print "Checking for scalable target for " + metric['operation'] + " operations"
                if scalable_target_exists(resource_id,metric[dimension]):
                    print "Scalable target exists for table " + resource_id + " for " + metric[dimension]
                else:
                    print "No scalable target exists for table " + resource_id + " for " + metric[dimension] + " - CREATING"
                    if register_scalable_target(resource_id,metric[dimension],role_arn,min_tput,max_tput):
                        print "Successfully registered scalable target for " + resource_id + " for " + metric[dimension]
                    else:
                        print "Failed to register scalable target for " + resource_id + " for " + metric[dimension]
                        status=False

                if scaling_policy_exists(resource_id,metric[dimension],metric['metric_type']):
                    print "Scaling policy exists for table " + resource_id + " for " + metric['metric_type']
                else:
                    print "No scaling policy exists for table " + resource_id + " for " + metric['metric_type'] + " - CREATING"
                    if put_scaling_policy(resource_id,metric['metric_type'],metric[dimension]):
                        print "Successfully registered scaling policy for table " + resource_id + " for " + metric['metric_type']
                    else:
                        print "Failed to register scaling policy for table " + resource_id + " for " + metric['metric_type']
                        status=False
    else:
        print "Unable to find role " + rolename + " in IAM - TERMINATING"
        status=False

    return status

def lambda_handler(event, context):
    status=True
    # print("Received event: " + json.dumps(event, indent=2))
    table_name = event['detail']['requestParameters']['tableName']
    table_resource_id='table/' + table_name

    # Check the Billing Mode of the table before trying to enable autoscaling
    print "Checking Billing Mode for table " + table_name
    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.describe_table(TableName=table_name)
    if 'Table' in response:
        if 'BillingModeSummary' in response['Table']:
            if response['Table']['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST':
                print("Table is set to on-demand capacity - no need to enable autoscaling")
                return True

    # Process the table
    if handle_resource(table_resource_id,'table'):
        print "Successfully handled resource " +  table_resource_id
    else:
        print "Errors handling resource " +  table_resource_id
        status=False

    # Does this table have indexes?  If so, we need to handle each one individually
    if 'globalSecondaryIndexes' in event['detail']['requestParameters']:
        for gsi in event['detail']['requestParameters']['globalSecondaryIndexes']:
            print "Processing global index " + gsi['indexName']
            index_resource_id='table/' + table_name + "/index/" + gsi['indexName']

            if handle_resource(index_resource_id,'index'):
                print "Successfully handled resource " +  index_resource_id
            else:
                print "Errors handling resource " +  index_resource_id
                status=False
    else:
        print "No indexes defined on table"

    return status
