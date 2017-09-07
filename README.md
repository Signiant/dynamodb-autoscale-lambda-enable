# dynamodb-autoscale-lambda-enable
Lambda function which verifies that dynamoDB autoscaling is enabled for a new table.  If it is found to not be enabled, it is enabled.

# Prerequsites
* An IAM role that allows DynamoDB autoscaling ([AWS Docs](https://goo.gl/JVmkGS))
# Deploying
* Zip up the lambda function code in the `lambda` folder and place in an S3 bucket
  * ie. `cd lambda; zip -r dynamodb-autoscale-enable.zip enable-as.py`
 * Create a new cloudformation stack using the template in the cfn folder

The stack asks for the function zip file location in S3, the role name (NOT the ARN), the minimum throughput and the maxium throughput.  Once the stack is created, a cloudwatch event is created to subscribe the lambda function to the `CreateTable` dynamodb call.
