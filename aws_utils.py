import aioboto3
from boto3.dynamodb.conditions import Attr
import env
import boto3

aioboto3_session = aioboto3.Session()
aws_credentials = {
    "aws_access_key_id": env.AWS_ACCESS_ID,
    "aws_secret_access_key": env.AWS_SECRET_KEY,
    "region_name": env.AWS_REGION,
}


async def dynamodb_get(tablename, key_dic):
    async with aioboto3_session.client(
        "dynamodb", **aws_credentials
    ) as dynamodb_client:
        ret = await dynamodb_client.get_item(
            TableName=tablename, Key=py_to_ddb(key_dic)
        )
        if "Item" not in ret:
            return None
        return ddb_to_py(ret["Item"])


async def dynamodb_update(tablename, key_dic, changes_dic, set_only=True):
    if set_only:
        changes_dic = {
            upd_key: {"Value": upd_val}
            for upd_key, upd_val in changes_dic.items()
        }
    ddb_changes_dic = {
        change_key: py_to_ddb(change_val)
        for change_key, change_val in changes_dic.items()
    }
    async with aioboto3_session.client(
        "dynamodb", **aws_credentials
    ) as dynamodb_client:
        await dynamodb_client.update_item(
            TableName=tablename,
            Key=py_to_ddb(key_dic),
            AttributeUpdates=ddb_changes_dic,
        )


async def dynamodb_delete(tablename, key_dic):
    async with aioboto3_session.client(
        "dynamodb", **aws_credentials
    ) as dynamodb_client:
        return await dynamodb_client.delete_item(
            TableName=tablename,
            Key=py_to_ddb(key_dic),
            ReturnValues="ALL_OLD",
        )


async def dynamodb_scan(
    table_name,
    filter_expression=None,
    is_in_list=None,
    is_in_attr="",
    ExpressionAttributeValues=None,
):
    if is_in_list != None:
        chunks = [
            is_in_list[x : x + 100] for x in range(0, len(is_in_list), 100)
        ]

        if len(chunks) == 0:
            return []
        filter_expression = Attr(is_in_attr).is_in(chunks[0])
        for i, chunk in enumerate(chunks):
            if i == 0:
                continue
            filter_expression = filter_expression | Attr(is_in_attr).is_in(
                chunk
            )

    async with aioboto3_session.client(
        "dynamodb", **aws_credentials
    ) as dynamodb_client:
        if filter_expression == None:
            response = await dynamodb_client.scan(TableName=table_name)
        else:
            response = await dynamodb_client.scan(
                TableName=table_name,
                FilterExpression=filter_expression,
                **{"ExpressionAttributeValues": ExpressionAttributeValues}
            )
        data_ret = response["Items"]
        while "LastEvaluatedKey" in response:
            if filter_expression == None:
                response = await dynamodb_client.scan(
                    TableName=table_name,
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                    **{"ExpressionAttributeValues": ExpressionAttributeValues}
                )
            else:
                response = await dynamodb_client.scan(
                    TableName=table_name,
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                    FilterExpression=filter_expression,
                    **{"ExpressionAttributeValues": ExpressionAttributeValues}
                )
            data_ret.extend(response["Items"])

        return [ddb_to_py(item) for item in data_ret]


async def create_table(table_name, key_name, key_type):
    async with aioboto3_session.client(
        "dynamodb", **aws_credentials
    ) as dynamodb_client:
        await dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": key_name, "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": key_name, "AttributeType": key_type},
            ],
            BillingMode="PAY_PER_REQUEST",
        )


async def dynamodb_truncate_table(table_name):
    counter = 0
    async with aioboto3_session.resource(
        "dynamodb", **aws_credentials
    ) as dynamodb_resource:
        table = await dynamodb_resource.Table(table_name)
        key_schema = await table.key_schema
        tableKeyNames = [key.get("AttributeName") for key in key_schema]
        projectionExpression = ", ".join("#" + key for key in tableKeyNames)
        expressionAttrNames = {"#" + key: key for key in tableKeyNames}

        response = await table.scan(
            ProjectionExpression=projectionExpression,
            ExpressionAttributeNames=expressionAttrNames,
        )
        counter += await __delete_all_from_batch(table, response)
        while "LastEvaluatedKey" in response:
            response = await table.scan(
                ProjectionExpression=projectionExpression,
                ExpressionAttributeNames=expressionAttrNames,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            counter += await __delete_all_from_batch(table, response)
        return counter


async def __delete_all_from_batch(table, response):
    async with table.batch_writer() as batch:
        for itemKeys in response["Items"]:
            await batch.delete_item(Key=itemKeys)
    return response["Count"]


async def s3_upload(im_path, bucket, relpath):
    async with aioboto3_session.client("s3", **aws_credentials) as s3_client:
        await s3_client.upload_file(im_path, bucket, relpath)


boto3.resource("dynamodb", **aws_credentials)


ddb_to_py = lambda ddb_data: {
    k: boto3.dynamodb.types.TypeDeserializer().deserialize(v)
    for k, v in ddb_data.items()
}

py_to_ddb = lambda py_data: {
    k: boto3.dynamodb.types.TypeSerializer().serialize(v)
    for k, v in py_data.items()
}
