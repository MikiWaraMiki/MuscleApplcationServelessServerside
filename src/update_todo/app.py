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
    """
    引数で受け取ったパラメータに不足がないかチェックするメソッド
    Parameters
    ----------
    post_parameter: Object
        パラメータで送られてきたデータ
    Returns
    --------
    True: Boolean
        パラメータに不足がない場合(パラメータが正常である場合)
    False: Boolean
        パラメータに不足がある場合
    """
    param_key_list = ("id","clear_plan", "name", "weight", "set", "user_name")
    for param_key in param_key_list:
        if param_key not in post_parameter:
            logger.info("{} is not exist in post parameter".format(param_key))
            return False
    return True


def decimal_default_proc(obj):
    """
    引数で受け取ったDecimalか判別し、Decimal型の場合はfloat型に変換する
    Parameters
    ----------
    obj : Any Object
        任意の型のオブジェクト
    Returns
    -------
    float(obj) : Float
        DecimalをFloat型に変換した値
    """
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def get_user_name_from_id_token(id_token):
    """
    引数で受け取ったJWTをデコードし、ユーザ名を取得するメソッド
    Paramters
    ---------
    id_token: string
        JWTで記載されたIDトークン
    Returns
    -------
    user_info["Cognito:user_name"] : string
        トークンから取得したユーザ名
    """
    logger.debug("id_token: {}".format(id_token))
    user_info = CognitoObject.get_user_info_from_id_token(id_token)
    logger.debug("Complete Decoding id Token")
    logger.debug(user_info)
    if "cognito:username" in user_info:
        return user_info["cognito:username"]
    else:
        return ""

def is_user_name_invalid(token_user_name = "", param_user_name = ""):
    """
    tokenから取得したユーザ名とデータに記載されたユーザ名が一致するか検証するメソッド
    Parameters
    ----------
    token_user_name : str
        Tokenから取得したユーザ名
    param_user_name : str
        HTTPパラメータに記載されたユーザ名
    Returns
    -------
    True : Boolean
        ユーザ名が一致する場合
    False : Boolean
        ユーザ名が一致しない場合
    """
    return True if token_user_name == param_user_name else False

def lambda_handler(event, context):
    """
    Todoを更新するメソッド
    引数で受けたったパラメータから指定されたTodoを更新する


    """
    logger.debug(event)
    post_parameter = json.loads(event["body"])
    
    try:
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
        # ユーザの正当性を確認する
        token = event["headers"]["Authorization"]
        # ユーザ名の取得
        user_name = get_user_name_from_id_token(token)
        logger.debug("User name in param {}".format(post_parameter["user_name"]))
        logger.debug("User name in token {}".format(user_name))
        # tokenの有効切れもしくはtokenから取得したユーザ名以外のデータを更新しようとしている場合は403を返す
        if  user_name == "" or not is_user_name_invalid(token_user_name = user_name, param_user_name = post_parameter["user_name"]):
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
        
        # リソース取得
        if os.getenv("AWS_SAM_LOCAL"):
            logger.debug("Development environment.")
        else:
            logger.debug("Production envrionment.")
        todo_db = Todo()
        # 更新用のパラメータを生成
        update_params = {
            "id": post_parameter["id"],
            "name": post_parameter["name"],
            "set": post_parameter["set"],
            "weight": post_parameter["weight"],
            "clear_plan": post_parameter["clear_plan"]
        }
        # Todoの更新
        logger.debug("todo(id:{}) is complete. Changing status".format(update_params["id"]))
        # 返却用データ
        response = todo_db.update_todo(update_params)
        #結果をリターン
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                'item': response
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