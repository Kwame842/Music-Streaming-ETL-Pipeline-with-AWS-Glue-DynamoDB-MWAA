import boto3

dynamodb = boto3.client("dynamodb", region_name="us-east-1")

response = dynamodb.create_table(
    TableName="ProcessedStreams",
    KeySchema=[
        {"AttributeName": "filename", "KeyType": "HASH"}
    ],
    AttributeDefinitions=[
        {"AttributeName": "filename", "AttributeType": "S"}
    ],
    BillingMode="PAY_PER_REQUEST"
)

print("Table creation initiated:", response["TableDescription"]["TableStatus"])
