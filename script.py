import pandas
import json
from boto3.dynamodb.types import TypeSerializer

def replaceObjectName(data):
    data = data.replace('"M"', '"m"')
    data = data.replace('"L"', '"l"')
    data = data.replace('"S"', '"s"')
    data = data.replace('"N"', '"n"')
    return data

def converterToDynamoFormat(data):
    typer = TypeSerializer()
    dynamodbJsonData = json.dumps(typer.serialize(data)['M'])
    return replaceObjectName(dynamodbJsonData)

df = pandas.read_csv('./data.csv', dtype=str)
df = df.fillna('')

user_number, columns_number = df.shape

print(user_number)
print(columns_number)

users = []

for user_index in range(user_number):
    print(' ---- reading ' + str(user_index) + ' ---- ')
    user = {}
    for columns_index in range(columns_number):
        column_name = df.columns[columns_index]
        user[column_name] = df[column_name][user_index]
    users.append(user)

i = 0
with open('./datacsv.txt', 'w+') as f:
    for user in users:
        print(' ---- writing ' + str(i) + ' ---- ')
        i = i + 1
        f.write(converterToDynamoFormat(user))
        f.write('\n')