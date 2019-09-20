import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.client import Config
from botocore.exceptions import ClientError

import os
from decimal import Decimal
from datetime import datetime, timedelta
from itertools import groupby

#logger オブジェクト
from logger_layer import ApplicationLogger
logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)

class DynamodbObject():
    def __init__(self, env_str):
        try:
            if env_str == "dev":
                logger.debug("Local DynamoDB")
                dynamodb_resource = boto3.resource("dynamodb", 
                    endpoint_url = os.getenv("DYNAMODB_ENDPOINT"),
                    region_name = "ap-northeast-2",
                    aws_access_key_id = os.getenv("DYNAMODB_ACCESS_ID"),
                    aws_secret_access_key = os.getenv("DYNAMODB_ACCESS_KEY")
                )
            else:
                dynamodb_resource = boto3.resource("dynamodb")
            logger.debug(dynamodb_resource)
        except ClientError as e:
            logger.warn('Dynamodb Error Occured')
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn('Error is Occured')
            logger.warn(e)
            raise
        self.dynamodb = dynamodb_resource
    
    def set_table(self,table_name):
        try:
            logger.debug("Table name is {}".format(table_name))
            self.table = self.dynamodb.Table(table_name)
            logger.debug("Table name {} is exist".format(table_name))
        except ClientError as e:
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn(e)
            raise
    
    def get_item(self, query):
        try:
            logger.info("Query:{}".format(query))
            db_response = self.table.scan(
                FilterExpression = query
            )
            return db_response
        except ClientError as e:
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn(e)
            raise

    def put_one_item(self,item):
        try:
            result = self.table.put_item(
                Item = item
            )
            return result
        except ClientError as e:
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn(e)
            raise

    def updateItem(self, key, update_query, attribute_values, attribute_names=''):
        try:
            logger.info("Key: {}".format(key))
            logger.info("update_query: {}".format(update_query))
            logger.info("attribute_values: {}".format(attribute_values))
            logger.debug("Updating Item")
            result = self.table.update_item(
                Key = key,
                UpdateExpression = update_query,
                ExpressionAttributeValues = attribute_values,
                ExpressionAttributeNames  = attribute_names,
                ReturnValues = "UPDATED_NEW"
            )
            logger.debug("Complete Item")
            return result
        except ClientError as e:
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn(e)
            raise

class Todo(DynamodbObject):
    def __init__(self,env_str):
        super().__init__(env_str)
        self.set_table('Todos')
        self.atomic_counter = TodoAtomicCounter(env_str)
        
    def get_all_todos(self,user_name=""):
        attribute_name = 'user_name'
        try:
            logger.debug("attribute_name: {} user_name:{}".format(attribute_name, user_name))
            query = Attr(attribute_name).eq(user_name)
            db_response = self.get_item(query)
            logger.debug("Response: {}".format(db_response))
            return db_response["Items"]
        except ClientError as e:
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn(e)
            raise

    def put_todo(self, todo):
        try:
            sequence_number = self.atomic_counter.countup_atomic_counter()
            todo['id'] = sequence_number
            todo['clear_plan'] = self.__convert_decimal_from_string_to_input_dynamodb(todo['clear_plan'])
            # データ登録日算出
            today = datetime.now().strftime("%Y-%m-%d")
            todo['created_at'] = self.__convert_decimal_from_string_to_input_dynamodb(today)

            #データ登録
            logger.debug("Start Put Item. Item: {}".format(todo))
            result = self.put_one_item(todo)
            return result
        except ClientError:
            raise
        except Exception:
            raise
    def complete_todo(self, todo):
        try:
            key = {
                'id': todo["id"]
            }
            update_query = "set is_cleared= :i, clear_date= :c, #com= :com"
            attribute_values = {
                ':i': True,
                ':c': self.__convert_decimal_from_string_to_input_dynamodb(todo["clear_date"]),
                ':com': todo["comment"]
            }
            attribute_names = {
                '#com': 'comment'
            }
            logger.debug("Change Status Complete")
            complete_response = self.updateItem(key, update_query, attribute_values, attribute_names)
            logger.debug(complete_response)
            return complete_response
        except ClientError as e:
            logger.warn("Client Error is Occured")
            raise
        except Exception as e:
            logger.warn("Exception is Occured")
            logger.warn(e)
            raise
    
    def get_muscle_menu_data(self, menu_name):
        try:
            query       = Attr('name').eq(menu_name) & Attr('is_cleared').eq(True)
            logger.debug("Get Items. Query: {}".format(query))
            db_response = self.get_item(query)
            return db_response["Items"]
        except ClientError as e:
            logger.warn("ClientError is occured. Reason is in the below")
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn("Some error is occured. Reason in the below")
            logger.warn(e)
            raise

    #private method
    def __decimal_default_proc(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError
    
    def __convert_datetime_from_decimal(self, decimal_timestamp):
        float_timestamp = float(decimal_timestamp)
        dt = datetime.fromtimestamp(float_timestamp)
        return dt.strftime("%Y-%m-%d")

    def __group_by_items(self,items, key_attribute):
        group_item_object = {}
        items.sort(key = lambda item: item[key_attribute])
        gropuby_items = groupby(items, key=lambda item: item[key_attribute])
        for key, group in gropuby_items:
            # 要素数を出すためにlistへ変換
            group_list = list(group)
            group_data_obj = {
                key: len(group_list)
            }
            group_item_object.update(group_data_obj)
        return group_item_object

    def __convert_decimal_from_string_to_input_dynamodb(self,string_datetime):
        tdatetime = datetime.strptime(string_datetime, '%Y-%m-%d')
        decimal_timestamp = Decimal(tdatetime.timestamp())
        return decimal_timestamp

class TodoAtomicCounter(DynamodbObject):
    def __init__(self, env_str):
        super().__init__(env_str)
        self.set_table('MuscleAtomicCounter')
    
    def countup_atomic_counter(self):
        try:
            logger.debug("Update atomic counter")
            sequence_number = self.table.update_item(
                Key={
                    'table_name': 'Todos'
                },
                UpdateExpression='ADD #name :increment',
                ExpressionAttributeNames={
                    '#name': 'current_number'
                },
                ExpressionAttributeValues={
                    ':increment': int(1)
                },
                ReturnValues='UPDATED_NEW'
            )
            logger.debug("Finished updating atomic counter. counter_value: {}".format(sequence_number))
            return sequence_number["Attributes"]["current_number"]
        except ClientError as e:
            logger.debug(e)
            raise
        except Exception as e:
            logger.debug(e)
            raise