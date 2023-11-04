### Libraries
import pandas as pd
import boto3 as b3

import json
from decimal import Decimal

### Functions
def turn_float_to_decimal(dict_item, key):
    dict_item[key] = Decimal(str(dict_item[key]))
    return dict_item

print('Loading the Airbnb data ...')
airbnb20 = pd.read_csv('https://raw.githubusercontent.com/chriszapp/datasets/main/airbnb_lisbon_1480_2017-07-27.csv')\
                .head(20)\
                .rename(columns = {'room_id': 'id'})

### Data Cleaning : drop unused columns, turn 'id' into str, and turn float into decimal with Decimal(str)
airbnb20_dict = airbnb20[['id', 'host_id', 'room_type', 'neighborhood', 'reviews', 'overall_satisfaction',
      'accommodates', 'bedrooms', 'price', 'name', 'latitude', 'longitude']]\
      .astype({'id': 'str',
          'bedrooms': 'int', 
          'price': 'int'
      })\
    .dropna()\
    .to_dict(orient = 'records')
airbnb20_dict = list(map(lambda room_dict: turn_float_to_decimal(room_dict, 'overall_satisfaction'), airbnb20_dict))
airbnb20_dict = list(map(lambda room_dict: turn_float_to_decimal(room_dict, 'latitude'), airbnb20_dict))
airbnb20_dict = list(map(lambda room_dict: turn_float_to_decimal(room_dict, 'longitude'), airbnb20_dict))
print(airbnb20_dict)

### Connecting to DynamoDB table
print('Connecting to the DynamoDB table ...')
table_name = 'hilxairbnbtable'
table = b3.resource('dynamodb').Table(table_name)

### Fill with items
with table.batch_writer() as batch:
    # For some reason map doesn't seem to work T-T
    # map(lambda room_rec: batch.put_item(Item = room_rec), airbnb20_dict)
    for i in range(len(airbnb20_dict)):
        batch.put_item(Item=airbnb20_dict[i])  


### Items Chack
print('Results: ', len(table.scan()['Items']))
