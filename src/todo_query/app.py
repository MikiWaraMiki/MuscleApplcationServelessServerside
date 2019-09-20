import os
import json
import boto3

from datetime import datetime
from decimal import Decimal
from itertools import groupby

from botocore.client import Config
from botocore.exceptions import ClientError

# 自作モジュール
from logger_layer import ApplicationLogger
from dynamodb_layer import Todo
from cognito_layer import CognitoObject

logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)

def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def convert_datetime_from_decimal(decimal_timestamp):
    float_timestamp = float(decimal_timestamp)
    dt              = datetime.fromtimestamp(float_timestamp)
    return dt.strftime("%Y-%m-%d")

def group_by_item(items, key_attribute):
    group_items_obj = {}
    items.sort(key = lambda item: item[key_attribute])
    gropuby_items = groupby(items, key=lambda item: item[key_attribute])
    for key, group in gropuby_items:
        # 要素数を出すためにlistへ変換
        group_list = list(group)
        group_data_obj = {
            key: len(group_list)
        }
        group_items_obj.update(group_data_obj)
    return group_items_obj

def create_chart_data(clear_todos):
    chart_data= [{"name":todo["name"],"clear_date":convert_datetime_from_decimal(todo["clear_date"])} for todo in clear_todos]
    pie_chart_data  = group_by_item(chart_data, "name")
    line_chart_data = group_by_item(chart_data, "clear_date")
    return {
        "pie": pie_chart_data,
        "line": line_chart_data
    }

def get_user_name_from_id_token(id_token):
    logger.debug("id_token: {}".format(id_token))
    user_info = CognitoObject.get_user_info_from_id_token(id_token)
    logger.debug("Complete Decoding id Token")
    logger.debug(user_info)
    return user_info["cognito:username"]

def lambda_handler(event, context):
    logger.debug(event)
    # table取得
    try:
        # リソース取得
        if os.getenv("AWS_SAM_LOCAL"):
            logger.debug("Development environment.")
            todo_db = Todo("dev")
        else:
            logger.debug("Production envrionment.")
            todo_db = Todo("prod")
        # response用
        non_clear_todos = []
        clear_todos     = []
        # Access Token 取得
        token       = event["headers"]["Authorization"]
        logger.debug("Verifing User Token")
        user_name   = get_user_name_from_id_token(token)
        # Item取得
        logger.info("Searching User Todos")
        db_response = todo_db.get_all_todos(user_name=user_name)
        if len(db_response) > 0:
            for item in db_response:
                item["clear_plan"] = convert_datetime_from_decimal(item["clear_plan"])
                item["created_at"] = convert_datetime_from_decimal(item["created_at"])
                if item["is_cleared"]:
                    clear_todos.append(item)
                else:
                    non_clear_todos.append(item)
        logger.debug("Finished searcing user todos.\n clear_todos num: {}\n non_clear_todos num: {}".format(len(clear_todos),len(non_clear_todos)))
        
        # トップページのグラフ用データを作成する
        chart_data = create_chart_data(clear_todos)
        logger.debug("Chart Data: {}".format(chart_data))
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                'data': {
                    "not_complete": non_clear_todos,
                    "complete": clear_todos,
                    "pie": chart_data["pie"],
                    "line": chart_data["line"]
                },
            }, default=decimal_default_proc)
        }
    except ClientError as e:
        logger.warn(e)
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                "message": "Failed. An Error Occured",
            })
        }
    except Exception as e:
        logger.warn("Get Todos is Falied")
        logger.warn("Reason\n {}".format(e))
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                'message': 'Failed. An Error Occured',
            })
        }
