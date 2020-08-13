import boto3
from boto3.dynamodb.conditions import Attr
dynamo_resource = None
dynamo_client = None


def init_dynamo():
    global dynamo_resource, dynamo_client
    if dynamo_resource is None:
        dynamo_resource = boto3.resource("dynamodb", region_name="eu-west-2")
        dynamo_client = boto3.client("dynamodb")


def write_mother_cat():
    init_dynamo()
    record = \
        {"partitionKey": "cat01", "name": "Molly", "age": 8, "nicknames": ["Molly-cat", "Missus", "Momma"]}
    table = dynamo_resource.Table("TEST_CATS")
    return table.put_item(Item=record)


def read_cat_by_name():
    init_dynamo()
    table = dynamo_resource.Table("TEST_CATS")

    done = False
    start_key = None
    items = []
    while not done:
        if start_key:
            response = table.scan(FilterExpression=Attr("name").eq("Pudding"), ExclusiveStartKey=start_key)
        else:
            response = table.scan(FilterExpression=Attr("name").eq("Pudding"))
        for item in response.get("Items", []):
            items.append(item)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    return items


def write_tx():
    init_dynamo()
    record = \
        {"partitionKey": {"S": "cat01"}, "name": {"S": "Molly"}, "age": {"N": "8"}, "nicknames": {"SS": ["Molly-cat", "Missus", "Momma"]}}
    return dynamo_client.transact_write_items(TransactItems=[{"Put": {"TableName": "TEST_CATS", "Item": record}}])
