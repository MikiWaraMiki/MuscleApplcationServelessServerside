import os
import json
# 自作モジュール
from logger_layer import ApplicationLogger
from dynamodb_layer import Todo


logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)

def lambda_handler(event, context):
    logger.debug(event)
    try:
        if event["httpMethod"] == "OPTIONS":
            return {
                'statusCode': 200,
                'headers': {
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": True,
                    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization, authorization,X-Api-Key,X-Amz-Security-Token"
                },
                'body':''
            }
        else:
            return {
                'statusCode': 400,
                'body': {
                    'message': 'this metdho is not supported. check your http method'
                }
            }

    except Exception as e:
        logger.debug(e)
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Methods": "*",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "AllowHeaders": "Content-Type,X-Amz-Date,authorization,X-Api-Key,X-Amz-Security-Token"
            },
            'body': json.dumps({
                'message': str(e),
            })
        }