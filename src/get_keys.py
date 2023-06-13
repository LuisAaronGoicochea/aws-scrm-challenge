import boto3
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "aws-secret-access"
    region_name = "us-east-1"

    # Accede a Secrets Manager
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    
    print(secret)
    
    # Accede a CodePipeline
    codepipeline_client = boto3.client('codepipeline')

    # Accede a CodeBuild
    codebuild_client = boto3.client('codebuild')
    
    return secret, codepipeline_client, codebuild_client