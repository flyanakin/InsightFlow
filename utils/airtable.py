import pandas as pd
import requests
import json

class Airtable:
    def __init__(self, api_key, base_id, table_name):
        self.api_key = api_key
        self.base_id = base_id
        self.table_name = table_name
        self.headers = {'Authorization': 'Bearer ' + api_key, 'Content-Type': 'application/json'}
        self.base_url = f'https://api.airtable.com/v0/{base_id}/{table_name}'

    def get_all_records(self):
        records = []
        url = self.base_url

        while True:
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                raise Exception('Failed to retrieve data: ' + response.text)
            data = response.json()
            records.extend(data['records'])

            if 'offset' in data:
                url = f'{self.base_url}?offset={data["offset"]}'
            else:
                break

        return records

    def clear_table(self):
        records = self.get_all_records()

        for record in records:
            response = requests.delete(f"{self.base_url}/{record['id']}", headers=self.headers)
            if response.status_code != 200:
                raise Exception('Failed to delete record: ' + response.text)

    def update_hold_table(self, df):
        for _, row in df.iterrows():
            data = {'fields': row.to_dict()}
            print(f'即将写入{data}')
            response = requests.post(self.base_url, headers=self.headers, data=json.dumps(data))

            if response.status_code != 200:
                raise Exception('Failed to post data: ' + response.text)
