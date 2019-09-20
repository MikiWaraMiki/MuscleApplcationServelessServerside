import os
import json
from datetime import datetime
from decimal import Decimal
from itertools import groupby

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# 自作モジュール
from logger_layer import ApplicationLogger
from dynamodb_layer import Todo
from cognito_layer import CognitoObject

logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)



def is_exist_parameter(post_parameter):
    param_key_list = ("id", "clear_date")
    for param_key in param_key_list:
        if param_key not in post_parameter:
            logger.info("{} is not exist in post parameter".format(param_key))
            return False
    return True



def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def get_user_name_from_id_token(id_token):
    logger.debug("id_token: {}".format(id_token))
    user_info = CognitoObject.get_user_info_from_id_token(id_token)
    logger.debug("Complete Decoding id Token")
    logger.debug(user_info)
    if "cognito:username" in user_info:
        return user_info["cognito:username"]
    else:
        return ""

def lambda_handler(event, context):
    logger.debug(event)
    post_parameter = json.loads(event["body"])
    # ユーザの正当性を確認する
    try:
        token = event["headers"]["Authorization"]
        # ユーザ名の取得
        user_name = get_user_name_from_id_token(token)
        if  user_name == "":
            logger.warn("User is invalid. Return 403 Code")
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
        # parameterが不足している場合は失敗を返す
        if is_exist_parameter(post_parameter) is False:
            logger.info("parameter is in valid")
            return {
                'statusCode': 422,
                'headers': {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": True,
                    "Access-Control-Allow-Headers": "*"
                },
                'body': json.dumps({
                    "message": "not set parameter.",
                })
            }
        # リソース取得
        if os.getenv("AWS_SAM_LOCAL"):
            logger.debug("Development environment.")
            todo_db = Todo("dev")
        else:
            logger.debug("Production envrionment.")
            todo_db = Todo()
        ## Todoの更新
        complete_info = {
            "id": int(post_parameter["id"]),
            "clear_date": post_parameter["clear_date"],
            "comment": post_parameter["comment"]
        }
        logger.debug("todo(id:{}) is complete. Changing status".format(complete_info["id"]))
        complete_resopnse = todo_db.complete_todo(complete_info)
        logger.debug("Success changing status. Resopnse  is in the below")
        logger.debug(complete_resopnse)
        # 返却用データ
        return_data = {
            "id": complete_info["id"],
            "clear_date": complete_resopnse["Attributes"]["clear_date"]
        }
        #結果をリターン
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                'item': return_data
            }, default=decimal_default_proc)
        }
    except ClientError as e:
        logger.warn("Failed changing status. ClientError is occured.")
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
        logger.warn("Failed changing status.Some Exception is occured")
        logger.warn(e)
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