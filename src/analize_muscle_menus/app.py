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
from dynamodb_layer import Todo
from cognito_layer import CognitoObject
from pandas_layer import TodoPandasObject
logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)


def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def get_user_name_from_id_token(id_token):
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
    post_parameters = json.loads(event["body"])
    # todo name
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
        # トレーニング名を取得
        if "name" not in post_parameters:
            return {
                'statusCode': 422,
                'headers': {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Credentials": True,
                    "Access-Control-Allow-Headers": "*"
                },
                'body': json.dumps({
                    "message": 'parameter is invalid.',
                })
            }
        # メニュー名
        menu_name = post_parameters["name"]
        # リソース取得
        if os.getenv("AWS_SAM_LOCAL"):
            logger.debug("Development environment.")
            todo_db = Todo("dev")
        else:
            logger.debug("Production envrionment.")
            todo_db = Todo()

        ## 解析データの取得
        logger.debug("Gathering data from DB")
        menu_dynamo_datas = todo_db.get_muscle_menu_data(menu_name)
        logger.debug("Gathered data. {}".format(menu_dynamo_datas))
        ## PandasObejct
        pd_object = TodoPandasObject(menu_dynamo_datas)
        #日付をフォーマット
        pd_object.df["clear_plan"] = pd_object.translate_to_datetime_from_decimal("clear_plan")
        pd_object.df["clear_date"] = pd_object.translate_to_datetime_from_decimal("clear_date")
        # Trend Dataを取得
        logger.debug("Getting trend data for weight")
        weight_trend_data          = pd_object.get_trend_data("weight")
        logger.debug("Completeing trend data for weight")
        logger.debug("Getting trend data for set")
        set_trend_data             = pd_object.get_trend_data("set")
        logger.debug("Completing trend data for set")
        return_data = {
            'weight': weight_trend_data,
            'set'   : set_trend_data
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