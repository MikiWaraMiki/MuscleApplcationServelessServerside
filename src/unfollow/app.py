import os
import json
from datetime import datetime

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# 自作モジュール
from logger_layer import ApplicationLogger
from cognito_layer import CognitoObject
from dynamodb_layer import FollowRelation


logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)

def get_user_name_from_id_token(id_token):
    """
    Cognito ID Tokenからユーザ名を取得するメソッド

    Paramters
    ----------
    id_token : string
        Cognito ID Token
    
    Returns
    ---------
    user_info["congnito:username"] : string
        ID Tokenから取得したユーザ名

    """
    logger.debug("id_token: {}".format(id_token))
    user_info = CognitoObject.get_user_info_from_id_token(id_token)
    logger.debug("Complete Decoding id Token")
    logger.debug("User name is {}".format(user_info['cognito:username']))
    if user_info is not "" and "cognito:username" in user_info:
        return user_info["cognito:username"]
    else:
        return ""
def lambda_handler(event, context):
    logger.debug(event)
    # ユーザの正当性を確認する
    try:
        # tokenを取得
        logger.debug("Authorization {}".format(event["headers"]["Authorization"]))
        token         = event["headers"]["Authorization"] if "Authorization" in event["headers"] else ""
        follower_name = get_user_name_from_id_token(token)
        if not follower_name:
            return {
                'statusCode': 403,
                'headers': {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": True,
                    "Access-Control-Allow-Headers": "*"
                },
                'body': json.dumps({
                    "message": 'Access is Denied',
                })
            }
        #relationを削除
        relation_id = event['pathParameters']['id']
        logger.debug("Delete Function is working.")
        relation_db  = FollowRelation()
        query_params = {
            'follower_name': follower_name,
            'relation_id': relation_id
        }
        result       = relation_db.delete_follow_relation(query_params = query_params)
        #結果をリターン
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                "message": "success unfollow"
            })
        }
    except Exception as e:
        logger.warn(e)
        logger.warn("Failed. Lambda function.")
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "AllowHeaders": "Content-Type,X-Amz-Date,authorization,X-Api-Key,X-Amz-Security-Token"
            },
            'body': json.dumps({
                'message': str(e),
            })
        }