import boto3
from botocore.exceptions import ClientError
import decimal
import json
from boto3.dynamodb.conditions import Key, Attr


def get_dynamodb_client():
    dynamodb_client = boto3.client("dynamodb", region_name="us-east-2", endpoint_url="http://localhost:8000")

    """ :type : pyboto3.dynamodb """
    return dynamodb_client


def get_dynamodb_resource():
    dynamodb_resource = boto3.resource("dynamodb", region_name="us-east-2", endpoint_url="http://localhost:8000")

    """ :type : pyboto3.dynamodb """
    return dynamodb_resource


def create_table():
    table_name = "Movies"

    attribute_definitions = [
        {
            "AttributeName": "year",
            "AttributeType": "N"
        },
        {
            "AttributeName": "title",
            "AttributeType": "S"
        }
    ]

    key_schema = [
        {
            "AttributeName": "year",
            "KeyType": "HASH"
        },
        {
            "AttributeName": "title",
            "KeyType": "RANGE"
        }
    ]

    initial_ops = {
        "ReadCapacityUnits": 10,
        "WriteCapacityUnits": 10
    }

    dynamodb_table_response = get_dynamodb_client().create_table(
        AttributeDefinitions=attribute_definitions,
        KeySchema=key_schema,
        TableName=table_name,
        ProvisionedThroughput=initial_ops
    )

    print("Created DynamoDB table: " + str(dynamodb_table_response))


def put_item_on_table():
    try:
        response = get_dynamodb_resource().Table("Movies").put_item(
            Item={
                "year": 2015,
                "title": "The Big New Movie",
                "info": {
                    "plot": "Nothing happens at all.",
                    "rating": decimal.Decimal(0)

                }
            }
        )

        print("A New Movie added to the collection successfully")
        print(str(response))

        return response

    except Exception as error:
        print(error)


def update_item_on_table():
    response = get_dynamodb_resource().Table("Movies").update_item(
        Key={
            "year": 2015,
            "title": "The Big New Movie"
        },
        UpdateExpression="set info.rating = :r, info.plot = :p, info.actors = :a",
        ExpressionAttributeValues={
            ":r": decimal.Decimal(3.5),
            ":p": "Everything happens all at once",
            ":a": ["Larry", "Moe","David"]
        },
        ReturnValues="UPDATED_NEW"
    )

    print("Updating existing movie was success")
    print(str(response))
    return response


def conditionally_update_an_item():
    try:
        response = get_dynamodb_resource().Table("Movies").update_item(
            Key={
                "year": 2015,
                "title": "The Big New Movie"
            },
            UpdateExpression="remove info.actors[0]",
            ConditionExpression="size(info.actors) >= :num",
            ExpressionAttributeValues={
                ":num": 3
            },
            ReturnValues="UPDATED_NEW"
        )
    except ClientError  as error:
        if error.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(error.response["Error"]["Message"])
        else:
            raise
    else:
        print("Updated item on table conditionally")
        print(str(response))


def get_item_on_table():
    try:
        reponse = get_dynamodb_resource().Table("Movies").get_item(
            Key={
                "year":2015,
                "title": "The Big New Movie"
            }

        )
    except ClientError as error:
        print(error.response["Error"]["Message"])
    else:
        item = reponse["Item"]
        print("got the item successfully")
        print(str(reponse))


def conditionally_delete_an_item():
    try:
        response = get_dynamodb_resource().Table("Movies").delete_item(
            Key={
                "year": 2015,
                "title": "The Big New Movie"
            },
            ConditionExpression="info.rating >= :val",
            ExpressionAttributeValues={
                ":val": decimal.Decimal(3)
            }
            
        )
    except ClientError  as error:
        if error.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(error.response["Error"]["Message"])
        else:
            raise
    else:
        print("Deleted item on table conditionally")
        print(str(response))


def insert_sample_data():
    table = get_dynamodb_resource().Table("Movies")

    with open("moviedata.json") as json_file:
        movies = json.load(json_file, parse_float=decimal.Decimal)

        for movie in movies:
            year = int(movie["year"])
            title = movie["title"]
            info = movie["info"]

            print("Adding movie", year, title)

            table.put_item(
                Item={
                    "year": year,
                    "title":title,
                    "info": info
                }
            )
        print("Sample movie data imported successfully")


def query_movie_released_in_1985():
    response = get_dynamodb_resource().Table("Movies").query(
        KeyConditionExpression=Key("year").eq(1985)
    )

    for movie in response["Items"]:
        print(movie["year"], ":", movie["title"])


def query_movie_with_extra_conditions():
    print("Movies from 1992 - title A-L, with genera and lead actor")

    response = get_dynamodb_resource().Table("Movies").query(
        ProjectionExpression="#yr, title, info.genres, info.actors[0]",
        ExpressionAttributeNames={"#yr": "year"},
        KeyConditionExpression=Key("year").eq(1992) & Key("title").between('A','L')
    )

    for movie in response["Items"]:
        print(movie)



if __name__ == "__main__":
    #create_table()
    #put_item_on_table()
    #update_item_on_table()
    #conditionally_update_an_item()
    #get_item_on_table()
    #conditionally_delete_an_item()
    #insert_sample_data()
    #query_movie_released_in_1985()
    query_movie_with_extra_conditions()