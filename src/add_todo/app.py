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


logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)

def is_exist_parameter(post_parameter):
    param_key_list = ("user_name", "name", "weight", "set", "clear_plan")
    for param_key in param_key_list:
        if param_key not in post_parameter:
            logger.info("{} is not exist in post parameter".format(param_key))
            return False
    return True

def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def auth_request_user_is_valid(user_name,token):
    try:
        aws_client = boto3.client('cognito-idp')
        logger.debug(aws_client)
        try:
            logger.debug("Token: {}".format(token))
            user = aws_client.get_user(AccessToken=token)
            logger.debug("User info: {}".format(user))
            if user_name == user["Username"]:
                return True
            else:
                logger.warn("payload user_name is not same requested user. token is invalid")
                return False
        except Exception:
            raise
    except Exception as e:
        logger.error("Error is Occured")
        logger.error(e)
        raise

def convert_datetime_from_decimal(decimal_timestamp):
    float_timestamp = float(decimal_timestamp)
    dt              = datetime.fromtimestamp(float_timestamp)
    return dt.strftime("%Y-%m-%d")

def lambda_handler(event, context):
    logger.debug(event["headers"])
    post_parameter = json.loads(event["body"])
    # ユーザの正当性を確認する
    try:
        user_name = post_parameter["user_name"]
        token     = post_parameter["access_token"]  if "access_token" in post_parameter else ""
        is_user_valid = auth_request_user_is_valid(user_name, token)
        if is_user_valid is not True:
            logger.warn(is_user_valid)
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
        ## 登録データの作成
        todo = {
            "user_name": post_parameter["user_name"],
            "name": post_parameter["name"],
            "weight": int(post_parameter["weight"]),
            "set": int(post_parameter["set"]),
            "clear_plan": post_parameter["clear_plan"],
            "is_cleared": False
        }
        put_result = todo_db.put_todo(todo)
        todo["clear_plan"] = convert_datetime_from_decimal(todo["clear_plan"])
        todo["created_at"] = convert_datetime_from_decimal(todo["created_at"])
        #結果をリターン
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                'item': todo
            }, default=decimal_default_proc)
        }
    except ClientError as e:
        logger.debug(e)
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "AllowHeaders": "Content-Type,X-Amz-Date,authorization,X-Api-Key,X-Amz-Security-Token"
            },
            'body': json.dumps({
                "message": str(e),
            })
        }
    except Exception as e:
        logger.debug(e)
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