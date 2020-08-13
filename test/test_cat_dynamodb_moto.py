import unittest
from decimal import Decimal
import uuid

import boto3
from moto import mock_dynamodb2

from src import cat_dynamodb


class CatDynamoDBMotoTest(unittest.TestCase):

    @mock_dynamodb2
    def test_write_mother_cat(self):
        cat_dynamodb.dynamo_resource = None
        boto3.setup_default_session()
        client = boto3.client("dynamodb", region_name='eu-west-2')
        client.create_table(
            TableName="TEST_CATS",
            KeySchema=[
                {"AttributeName": "partitionKey", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "partitionKey", "AttributeType": "S"},
            ]
        )

        response = cat_dynamodb.write_mother_cat()
        self.assertEqual({'CapacityUnits': 1, 'TableName': 'TEST_CATS'}, response["ConsumedCapacity"])
        self.assertEqual(200, response["ResponseMetadata"]["HTTPStatusCode"])
        self.assertEqual(
            {'partitionKey': {'S': 'cat01'}, 'name': {'S': 'Molly'}, 'age': {'N': '8'},
             'nicknames': {'L': [{'S': 'Molly-cat'}, {'S': 'Missus'}, {'S': 'Momma'}]}
             },
            client.get_item(TableName="TEST_CATS", Key={"partitionKey": {"S": "cat01"}})["Item"]
        )

    @mock_dynamodb2
    def test_read_cat_by_name(self):
        cat_dynamodb.dynamo_resource = None
        boto3.setup_default_session()
        client = boto3.client("dynamodb", region_name='eu-west-2')
        client.create_table(
            TableName="TEST_CATS",
            KeySchema=[
                {"AttributeName": "partitionKey", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "partitionKey", "AttributeType": "S"},
            ]
        )
        client.put_item(
            TableName="TEST_CATS",
            Item={'partitionKey': {'S': 'cat02'}, 'name': {'S': 'Pudding'}, 'age': {'N': '2'},
                  'nicknames': {'L': [{'S': 'Puddy'}]}})

        self.assertEqual(
            [{'partitionKey': 'cat02', 'name': 'Pudding', 'age': Decimal(2), 'nicknames': ['Puddy']}],
            cat_dynamodb.read_cat_by_name())

    @mock_dynamodb2
    def test_write_tx(self):
        cat_dynamodb.dynamo_resource = None
        boto3.setup_default_session()
        client = boto3.client("dynamodb", region_name='eu-west-2')
        client.create_table(
            TableName="TEST_CATS",
            KeySchema=[
                {"AttributeName": "partitionKey", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "partitionKey", "AttributeType": "S"},
            ]
        )

        response = cat_dynamodb.write_tx()
        self.assertEqual({'CapacityUnits': 1, 'TableName': 'TEST_CATS'}, response["ConsumedCapacity"])
        self.assertEqual(200, response["ResponseMetadata"]["HTTPStatusCode"])
        self.assertEqual(
            {'partitionKey': {'S': 'cat01'}, 'name': {'S': 'Molly'}, 'age': {'N': '8'},
             'nicknames': {'L': [{'S': 'Molly-cat'}, {'S': 'Missus'}, {'S': 'Momma'}]}
             },
            client.get_item(TableName="TEST_CATS", Key={"partitionKey": {"S": "cat01"}})["Item"]
        )
