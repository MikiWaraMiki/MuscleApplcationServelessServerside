import os
import json
from datetime import datetime
from decimal import Decimal

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# 自作モジュール
from logger_layer import ApplicationLogger
from dynamodb_layer import Todo, FollowRelation
from cognito_layer import CognitoObject
from pandas_layer import TodoPandasObject
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

def response_403():
    """
    403コードのレスポンスパラメータ

    Returns
    -------
    unauthorized_response : dicstionary
        レスポンスパラメータ
    """
    unauthorized_response = {
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
    return unauthorized_response
def lambda_handler(event, context):
    logger.debug(event)
    # todo name
    try:
        token = event["headers"]["Authorization"]
        # ユーザ名の取得
        user_name = get_user_name_from_id_token(token)
        if  user_name == "":
            logger.warn("User is invalid. Return 403 Code")
            return response_403()
        # リソース取得
        if os.getenv("AWS_SAM_LOCAL"):
            logger.debug("Development environment.")
        else:
            logger.debug("Production envrionment.")
        todo_db = Todo()
        follow_relation_db = FollowRelation()
        ## 解析データの取得
        logger.debug("Getting timelines")
        #　タイムラインの取得
        timeline_datas  = todo_db.get_clear_todos_within_a_month(ago_days=-14)
        # フォローしているユーザを取得
        following_datas = follow_relation_db.get_following_users_queried_by_user_name(user_name= user_name)
        # Pandas生成
        pd_object     = TodoPandasObject(timeline_datas)
        timeline_datas = pd_object.create_timeline_data(following_datas)
        logger.debug(type(timeline_datas))
        # Trend Dataを取得
        #結果をリターン
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                'item': timeline_datas
            })
        }
    except ClientError as e:
        logger.warn("Failed. ClientError is occured.")
        logger.warn(e)
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "AllowHeaders": "Content-Type,X-Amz-Date,authorization,X-Api-Key,X-Amz-Security-Token"
            },
            'body': json.dumps({
                "message": "Failed. Error is Occured",
            })
        }
    except Exception as e:
        logger.warn("Failed.Some Exception is occured")
        logger.warn("Exception Class: {}".format(type(e)))
        logger.warn("message: {0}".format(e.args))
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "AllowHeaders": "Content-Type,X-Amz-Date,authorization,X-Api-Key,X-Amz-Security-Token"
            },
            'body': json.dumps({
                'message': "Failed. Error is Occured.",
            })
        }