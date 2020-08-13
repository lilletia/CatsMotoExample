import unittest
from unittest.mock import Mock
from boto3.dynamodb.conditions import Attr

from src import cat_dynamodb


class CatDynamoDBMockTest(unittest.TestCase):

    def test_write_mother_cat(self):
        mock_dynamodb_table = Mock()
        cat_dynamodb.dynamo_resource = Mock()
        cat_dynamodb.dynamo_resource.Table = Mock()
        cat_dynamodb.dynamo_resource.Table.return_value = mock_dynamodb_table
        mock_dynamodb_table.put_item = Mock()
        mock_dynamodb_table.put_item.return_value = {"success": "true"}

        self.assertEqual({"success": "true"}, cat_dynamodb.write_mother_cat())

        cat_dynamodb.dynamo_resource.Table.assert_called_once_with("TEST_CATS")
        mock_dynamodb_table.put_item.assert_called_once_with(
            Item={"partitionKey": "cat01", "name": "Molly", "age": 8,
                  "nicknames": ["Molly-cat", "Missus", "Momma"]}
        )

    def test_read_cat_by_name(self):
        mock_dynamodb_table = Mock()
        cat_dynamodb.dynamo_resource = Mock()
        cat_dynamodb.dynamo_resource.Table = Mock()
        cat_dynamodb.dynamo_resource.Table.return_value = mock_dynamodb_table
        mock_dynamodb_table.scan = Mock()
        mock_dynamodb_table.scan.return_value = \
            {"Items": [{"partitionKey": "cat02", "name": "Pudding", "age": 2, "nicknames": ["Puddy"]}]}

        self.assertEqual(
            [{"partitionKey": "cat02", "name": "Pudding", "age": 2, "nicknames": ["Puddy"]}],
            cat_dynamodb.read_cat_by_name()
        )

        cat_dynamodb.dynamo_resource.Table.assert_called_once_with("TEST_CATS")
        mock_dynamodb_table.scan.assert_called_once_with(FilterExpression=Attr("name").eq("Pudding"))
