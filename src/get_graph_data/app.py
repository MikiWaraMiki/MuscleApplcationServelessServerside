import os
import json
from datetime import datetime
from decimal import Decimal
from itertools import groupby

from botocore.client import Config
from botocore.exceptions import ClientError

# 自作モジュール
from logger_layer import ApplicationLogger
from dynamodb_layer import Todo

def create_datetime_from_time_stamp(time_stmap):
    dt = datetime.fromtimestamp(time_stmap)
    return dt.strftime("%Y-%m-%d")

def convert_decimal_in_data(items):
    if len(items) == 0 :
        return []
    convert_items_list = []
    for item in items:
        clear_date = float(item["clear_date"])
        convert_item = {
            "name": item["name"],
            "clear_date": create_datetime_from_time_stamp(clear_date)
        }
        convert_items_list.append(convert_item)
    return convert_items_list

def group_by_item(items, key_attribute):
    group_item_map = {}
    items.sort(key=lambda item: item[key_attribute])
    group_by_item = groupby(items, key=lambda item: item[key_attribute])
    for key, group in group_by_item:
        group_list = list(group)
        item_map = {
            key: len(group_list)
        }
        group_item_map.update(item_map)
    return group_item_map
    
def lambda_handler(event, context):
    # table取得
    try:
        # リソース取得
        if os.getenv("AWS_SAM_LOCAL"):
            logger.debug("Development environment.")
            todo_db = Todo("dev")
        else:
            logger.debug("Production envrionment.")
            todo_db = Todo()
        # response用
        pie_graph_datas  = []
        line_graph_datas = [] 
        # Access Token 取得
        user_name  = event["pathParameters"]["user_name"]
        todo_table = dynamodb_resouse.Table('Todos')
        # Item取得
        db_response = todo_table.scan(
            FilterExpression=Attr('user_name').eq(user_name) & Attr('is_cleared').eq(True)
        )
        # 円グラフデータの生成 トレーニングメニューでgroupby
        items = convert_decimal_in_data(db_response["Items"])
        if len(items) > 0:
            pie_graph_datas  = group_by_item(items, "name")
            line_graph_datas = group_by_item(items, "clear_date")
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                'data': {
                    "pie_data" : pie_graph_datas,
                    "line_data": line_graph_datas
                },
            })
        }
        
    except ClientError as e:
        logger.debug(e)
        return {
            'statusCode': 500,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "Access-Control-Allow-Headers": "*"
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
                "Access-Control-Allow-Headers": "*"
            },
            'body': json.dumps({
                'message': str(e),
            })
        }