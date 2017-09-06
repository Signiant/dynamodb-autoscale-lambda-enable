import boto3
import json
import sys,os
import pprint

metric_info=[
    {
        "operation": "read",
        "scaleable_dimension": "dynamodb:table:ReadCapacityUnits",
        "metric_type": "DynamoDBReadCapacityUtilization"
    },
    {
        "operation": "write",
        "scaleable_dimension": "dynamodb:table:WriteCapacityUnits",
        "metric_type": "DynamoDBWriteCapacityUtilization"
    }
]

def get_policy_name(table_name,metric_type):
    policy_name=metric_type + ":table/" + table_name
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

def scalable_target_exists(table_name,scalable_dimension):
    exists=False
    response=None

    print "Checking if scalable target exists for " + table_name + " for dimension " + scalable_dimension
    client = boto3.client('application-autoscaling')

    try:
        response = client.describe_scalable_targets(
            ServiceNamespace='dynamodb',
            ResourceIds=[
                'table/' + table_name,
            ],
            ScalableDimension=scalable_dimension
        )
    except Exception, e:
        print "Failed to describe scalable targets " + str(e)

    if response:
        if response['ScalableTargets']:
            exists=True

    return exists

def register_scalable_target(table_name,scalable_dimension,role_arn,min_tput,max_tput):
    status=False
    response=None

    print "Registering scalable target for " + table_name + " for dimension " + scalable_dimension
    client = boto3.client('application-autoscaling')

    try:
        response = client.register_scalable_target(
            ServiceNamespace='dynamodb',
            ResourceId='table/' + table_name,
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

def scaling_policy_exists(table_name,scalable_dimension,metric_type):
    exists=False
    response=None
    policy_name=get_policy_name(table_name,metric_type)

    print "Checking if scaling policy exists with name  " + policy_name

    if policy_name:
        client = boto3.client('application-autoscaling')

        try:
            response = client.describe_scaling_policies(
                PolicyNames=[
                    policy_name,
                ],
                ServiceNamespace='dynamodb',
                ResourceId='table/' + table_name,
                ScalableDimension=scalable_dimension
            )
        except Exception, e:
            print "Failed to describe scaling policies " + str(e)

        if response:
            if response['ScalingPolicies']:
                exists=True

    return exists

def put_scaling_policy(table_name,metric_type,scalable_dimension):
    print "put_scaling_policy"

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

def main(argv):
    table_name='DEVOPS_SES_DELIVERIES'  # TODO get this from the event
    rolename=os.environ['rolename']
    max_tput=os.environ['max_tput']
    min_tput=os.environ['min_tput']

    role_arn=get_role_arn(os.environ['rolename'])
    print "Role ARN is %s" % role_arn

    if role_arn:
        print "Role " + rolename + " found in IAM"

        # Now see if the items we need exist and create if not
        for metric in metric_info:
            print "Checking for scalable target for " + metric['operation'] + " operations"
            if scalable_target_exists(table_name,metric['scaleable_dimension']):
                print "Scalable target exists for table " + table_name + " for " + metric['scaleable_dimension']
            else:
                print "No scalable target exists for table " + table_name + " for " + metric['scaleable_dimension'] + " - CREATING"
                if register_scalable_target(table_name,metric['scaleable_dimension'],role_arn,min_tput,max_tput):
                    print "Successfully registered scalable target for " + table_name + " for " + metric['scaleable_dimension']
                else:
                    print "Failed to register scalable target for " + table_name + " for " + metric['scaleable_dimension']

            if scaling_policy_exists(table_name,metric['scaleable_dimension'],metric['metric_type']):
                print "Scaling policy exists for table " + table_name + " for " + metric['metric_type']
            else:
                print "No scaling policy exists for table " + table_name + " for " + metric['metric_type'] + " - CREATING"
    else:
        print "Unable to find role " + os.environ['rolename'] + " in IAM - TERMINATING"

if __name__ == "__main__":
   main(sys.argv[1:])
