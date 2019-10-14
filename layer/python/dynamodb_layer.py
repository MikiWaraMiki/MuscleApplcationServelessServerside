import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.client import Config
from botocore.exceptions import ClientError

import os
from decimal import Decimal
from datetime import datetime, timedelta
from itertools import groupby
import decimal
#logger オブジェクト
from logger_layer import ApplicationLogger
logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)

class DynamodbObject():
    def __init__(self):
        try:
            if os.getenv("AWS_SAM_LOCAL"):
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
    
    def get_item(self, query={}):
        """
        テーブルに対してScanをかけるメソッド。
        抽出条件(query)が指定されている場合はQueryをかける

        Parameters
        ------------
        query : Map
            クエリの条件
        Returns
        --------
         db_response : Dynamodb
            Scanの結果
        """
        try:
            logger.info("Query:{}".format(query))
            if query:
                db_response = self.table.scan(
                    FilterExpression = query["FilterExpression"],
                    ExpressionAttributeValues = query["ExpressionAttributeValues"],
                    ExpressionAttributeNames  = query["ExpressionAttributeNames"]
                )
            else:
                db_response = self.table.scan()
            return db_response
        except ClientError as e:
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn(e)
            raise

    def query_item(self, key_condition_expression=""):
        """
        テーブルに対してQueryを実行する。
        
        Parameters
        ----------
        key_condition_expression : boto3.dynamodb.conditions.Equals
            テーブルへのクエリ条件

        Returns
        result : Dictionary
            DynamoDBのレスポンス
        """
        try:
            result = self.table.query(
                KeyConditionExpression = key_condition_expression
            )
            return result
        except ClientError as e:
            logger.warn("Query is Failed")
            logger.warn("ClientError is occured")
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn("Query is Failed")
            logger.warn("Exception is occured")
            logger.warn(e)
            raise

    def get_item_from_gsi(self, params):
        try:
            logger.debug("Query Params: {}".format(params))
            if "FilterExpression" in params:
                logger.debug("Query has Fileter Expression. Filter {}".format(params["FilterExpression"]))
                result = self.table.query(
                    IndexName                 = params["IndexName"],
                    KeyConditionExpression    = params["KeyConditionExpression"],
                    FilterExpression          = params["FilterExpression"],
                    ExpressionAttributeValues = params["ExpressionAttributeValues"],
                    ExpressionAttributeNames  = params["ExpressionAttributeNames"]
                )
            else:
                result = self.table.query(
                    IndexName                 = params["IndexName"],
                    KeyConditionExpression    = params["KeyConditionExpression"],
                    ExpressionAttributeValues = params["ExpressionAttributeValues"],
                    ExpressionAttributeNames  = params['ExpressionAttributeNames']
                )
            return result
        except ClientError as e:
            logger.warn("ClientError is occured")
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn("OtherError is occured")
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
            if attribute_names:
                logger.info("attirbute_names {}".format(attribute_names))
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

    def delete_one_item(self, delete_parameter = {}):
        """
        テーブルから1件のデータを削除する

        Parameters
        ----------
        delete_parameter : dict
            削除するデータの条件が格納された辞書

        delete_result : dict
            削除結果が格納
        """

        try:
            delete_result = self.table.delete_item(
                Key = delete_parameter["key"]
            )
            return delete_result
        except ClientError as e:
            logger.warn("Clinet Error is Occured")
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn("Some Expetion is Occured")
            logger.warn(e)
            raise
class Todo(DynamodbObject):
    def __init__(self,env_str=""):
        super().__init__()
        self.set_table('Todos')
        self.atomic_counter = TodoAtomicCounter(env_str)
        self.clear_date_lsi = 'ClearDateLSIndex'
        self.user_name_gsi  = 'UserNameGSIndex'

    def get_all_todos(self,user_name=""):
        now            = datetime.now()
        gt_iso_format  = self.__convert_isoformat_string_from_datetime(now)
        lt_iso_format  = self.__convert_isoformat_string_from_datetime(now + timedelta(days = -30))
        try:
            if user_name:
                logger.debug("Getting {} todos".format(user_name))
                index_name           = 'UserNameCreatedGSIndex'
                key_condition_exp    = '#UN = :Un AND #CreateD BETWEEN :LT AND :GT'
                exp_attribute_values = {
                    ':Un': user_name,
                    ':LT': lt_iso_format,
                    ':GT': gt_iso_format 
                }
                exp_attribute_names  = {
                    '#UN': 'user_name',
                    '#CreateD': 'created_at' 
                }
                query = {
                    'IndexName':                 index_name,
                    'KeyConditionExpression':    key_condition_exp,
                    'ExpressionAttributeValues': exp_attribute_values,
                    'ExpressionAttributeNames':  exp_attribute_names
                }
                db_response = self.get_item_from_gsi(query)
                logger.debug("Response: {}".format(db_response))
                return db_response["Items"]
        except ClientError as e:
            logger.warn("DynamodbCall Failed")
            logger.warn(e)
            raise
        except Exception as e:
            logger.warn("DynamodbCall Failed")
            logger.warn(e)
            raise
    
    def get_clear_todos_within_a_month(self, ago_days=0):
        """
        今日の日付からから引数で受けっとった日数前のの完了ずみTodo一覧を取得するメソッド

        Parameters
        ------------
        ago_days : int
            取得する期間(X日前)
        Returns
        -------
        clear_todos : list
            完了ずみTodoの配列
        """
        try:
            today                    = datetime.now()
            gte_time_iso        = self.__convert_isoformat_string_from_datetime(today + timedelta(days = ago_days))
            filter_expression = "#ClearD >= :T1"
            ex_attribute_values      = {
                ':T1': gte_time_iso
            }
            ex_attibute_names        = {
                '#ClearD': 'clear_date'
            }
            query                    = {
                'FilterExpression': filter_expression,
                'ExpressionAttributeValues': ex_attribute_values,
                'ExpressionAttributeNames': ex_attibute_names
            }
            db_response              = self.get_item(query=query)
            logger.debug("response {}".format(db_response))

            return db_response["Items"]
        except Exception as e:
            logger.warn("Failed.")
            raise

    def put_todo(self, todo):
        try:
            sequence_number = self.atomic_counter.countup_atomic_counter()
            todo['id'] = int(sequence_number)
            #完了予定日のフォーマットをISOに変換
            complete_plan_date = datetime.strptime(todo['clear_plan'], '%Y-%m-%d')
            todo['clear_plan'] = self.__convert_isoformat_string_from_datetime(complete_plan_date)
            # データ登録日算出
            todo['created_at'] = self.__convert_isoformat_string_from_datetime(datetime.now())
            todo['clear_date'] = "0"
            #データ登録
            logger.debug("Start Put Item. Item: {}".format(todo))
            result = self.put_one_item(todo)
            logger.debug("Result {}".format(result))
            return todo
        except ClientError:
            raise
        except Exception:
            raise
    
    def update_todo(self, todo):
        try:
            #文字列をdate型に変換する
            clear_plan       = datetime.strptime(todo['clear_plan'], '%Y-%m-%d')
            key              = {
                'id': todo['id']
            }
            update_query     = "set #CP = :cp, #S = :s, #W = :w, #N = :n"
            attribute_values = {
                ':cp': self.__convert_isoformat_string_from_datetime(clear_plan),
                ':s' : todo['set'],
                ':w' : todo['weight'],
                ':n' : todo['name']
            }
            attribute_names = {
                '#CP': 'clear_plan',
                '#S': 'set',
                '#W': 'weight',
                '#N': 'name'
            }

            # データの更新
            update_response = self.updateItem(key, update_query, attribute_values, attribute_names)
            logger.debug("Response is in the below.")
            logger.debug(update_response)

            # 返却用のデータ
            clear_date_str = clear_plan.strftime('%Y-%m-%d')
            response_data = {
                'id': todo['id'],
                'name': todo['name'],
                'weight': todo['weight'],
                'set': todo['set'],
                'clear_plan': clear_date_str
            }
            return response_data
        except ClientError as e:
            logger.warn("Client error is occured.")
            raise
        except Exception as e:
            logger.warn("Some error is occured.")
            raise
    def complete_todo(self, todo):
        try:
            # 文字列をdate型に変換
            complete_date = datetime.strptime(todo["clear_date"], '%Y-%m-%d')
            key = {
                'id': todo["id"],
            }
            update_query = "set is_cleared= :i, clear_date= :c, #com= :com"
            attribute_values = {
                ':i': True,
                ':c': self.__convert_isoformat_string_from_datetime(complete_date),
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
    
    def get_muscle_menu_data(self, user_name, menu_name):
        try:
            # Query
            index_name           = 'UserNameCreatedGSIndex'
            # Filter   
            key_condition_exp    = '#UN = :un'
            filter_exp           = '#ClearD > :lt AND #MN = :mn'
            # Filter Values
            exp_attribute_values = {
                ':un': user_name,
                ':lt': '0',
                ':mn': menu_name
            }
            exp_attribute_names  = {
                '#UN': 'user_name',
                '#ClearD': 'clear_date',
                '#MN': 'name'
            }
            params = {
                "IndexName": index_name,
                "KeyConditionExpression": key_condition_exp,
                "FilterExpression": filter_exp,
                "ExpressionAttributeNames": exp_attribute_names,
                "ExpressionAttributeValues": exp_attribute_values
            }
            logger.debug("Query Start. pamras {}".format(params))
            db_response = self.get_item_from_gsi(params)
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
    
    def __convert_isoformat_string_from_datetime(self, dt):
        return dt.isoformat(timespec='microseconds')

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

class AtomicCounter(DynamodbObject):
    def __init__(self, env_str="", table_name=""):
        super().__init__()
        self.table_name = table_name
        self.set_table("MuscleAtomicCounter")
    
    def countup_atomic_counter(self):
        try:
            logger.debug("Update atomic counter")
            sequence_number = self.table.update_item(
                Key={
                    'table_name': self.table_name
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
class TodoAtomicCounter(AtomicCounter):
    def __init__(self,env_str=""):
        super().__init__(env_str="", table_name="Todos")

class FollowRelationAtomicCounter(AtomicCounter):
    def __init__(self):
        super().__init__(env_str="", table_name="FollowRelation")

class FollowRelation(DynamodbObject):
    def __init__(self):
        """
        コンストラクタ
        親クラスのコンストラクタを呼び出しDynamodbオブジェクトを取得する
        """
        super().__init__()
        self.set_table('FollowRelation')
        self.atomic_counter = FollowRelationAtomicCounter()

    def scan_all_data(self):
        """
        FollowRelationテーブル内の全てのデータを取得するメソッド

        Returns
        --------
        items : list
            DBから取得したデータが格納された配列
        """
        try:
            logger.debug("Scan Starting")
            db_response = self.get_item()
            items       = db_response["Items"]
            logger.debug("Complete Scanning. {} items".format(len(items)))
            return items
        except Exception as e:
            print("Scan failed.")
        
    def get_following_users_queried_by_user_name(self,user_name=""):
        """
        引数で受け取ったユーザがフォローしているユーザの一覧を取得する

        Parameters
        ---------
        user_name : string
            ユーザ名
        
        Returns
        -------
        following_users : list
            パラメータで受け取ったユーザがフォローしているユーザ名のリスト
        """

        try:
            logger.debug("Query Starting")
            key_condition_expression = Key('follower_name').eq(user_name)
            db_response = self.query_item(key_condition_expression = key_condition_expression)
            items       = db_response["Items"]
            logger.debug("Complete Query. {} items".format(len(items)))
            logger.debug(items)
            return items
        except Exception as e:
            logger.debug("Failed")
            raise
    def put_follow_relation(self, follow_parameter):
        """
        新しくフォローのデータを追加する

        Parameters
        ----------
        follow_parameter : dict
            登録データが格納
        
        Returns
        ---------
        put_result : dict
            登録結果
        """

        # パラメータの検証
        if "follower_name" not in follow_parameter or "following_name" not in follow_parameter:
            return ""

        # データの登録
        try:
            # atomicカウンターの更新
            sequence_number = self.atomic_counter.countup_atomic_counter()
            follow_parameter["created_at"] = datetime.now().isoformat()
            follow_parameter["id"] = sequence_number
            put_result = self.put_one_item(follow_parameter)
            logger.debug(put_result)
            follow_parameter["id"] = float(follow_parameter["id"])
            return follow_parameter
        except Exception as e:
            logger.warn("Put is Failed")
            raise

    def delete_follow_relation(self, query_params = {}):
        """
        既存のフォローデータを削除する

        Parameters
        ----------
        follow_parameter : string
            登録データが格納
        
        Returns
        ---------
        put_result : dict
            登録結果
        """
        # パラメータの検証
        if not query_params:
            return ""
        try:
            params = {
                'key': {
                    'follower_name': query_params['follower_name'],
                    'id': int(query_params['relation_id'])
                }
            }
            logger.debug("Delete data {} from FollowRelation".format(params))
            result        = self.delete_one_item(params)
            logger.debug("complete deleting todo {}".format(result))
            return True
        except Exception:
            logger.warn("Failed delete relation item.")
            raise