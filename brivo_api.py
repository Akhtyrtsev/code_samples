from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
import requests
from datetime import datetime, timedelta
from django.conf import settings
import json
import base64
import ast
import os
import random

base64_brivo_credentials = b"Basic " + base64.b64encode(
    str.encode("{}:{}".format(settings.BRIVO_CLIENT_ID, settings.BRIVO_CLIENT_SECRET)))


class BrivoApi:
    update_token_url = 'https://auth.brivo.com/oauth/token?grant_type=refresh_token&refresh_token={}'
    base_url = 'https://api.brivo.com/v1/api/'
    brivo_group_prefix = 'Tenants'

    def __init__(self, path_to_creds):
        self.path_to_creds = path_to_creds
        if os.path.exists(self.path_to_creds):
            with open(path_to_creds, 'r') as token:
                self.data = json.load(token)
                self.expires_in = self.data['expires_in']
                self.access_token = self.data['access_token']
                self.refresh_token = self.data['refresh_token']

        if getattr(self, 'expires_in', False) and self.is_expired():
            self.update_token()

    def is_expired(self):
        now = datetime.utcnow().timestamp()
        return now >= self.expires_in

    def update_token(self):
        now = datetime.utcnow().timestamp()
        url = self.update_token_url.format(self.data['refresh_token'])
        r = requests.post(url, headers={'Authorization': base64_brivo_credentials, 'api-key': settings.BRIVO_API_KEY})
        token = ast.literal_eval(r.content.decode())
        self.save_token(token, now)

    def save_token(self, data, now):
        self.expires_in = data['expires_in']
        self.access_token = data['access_token']
        self.refresh_token = data['refresh_token']
        with open(self.path_to_creds, 'w') as token:
            data['expires_in'] = now + data['expires_in']
            json.dump(data, token)

    def create_user(self, data):
        retries = 3
        url = self.base_url + 'users'
        for retry in range(retries):
            data['pin'] = random.randint(1000, 9999)                                                    # 4 digit pin
            r = requests.post(url, headers={
                'api-key': settings.BRIVO_API_KEY,
                'Authorization': f'bearer {self.access_token}',
                'Content-type': 'application/json'
            }, data=json.dumps(data))
            if r.status_code == 200:
                break
        return json.loads(r.content)

    def update_user(self, user_id, data):
        url = self.base_url + 'users/' + str(user_id)
        r = requests.put(url, headers={
            'api-key': settings.BRIVO_API_KEY,
            'Authorization': f'bearer {self.access_token}',
            'Content-type': 'application/json'
        }, data=json.dumps(data))
        if r.status_code in [404, 400]:
            print("User was not updated, user not found.")
        else:
            print("User is updated.")

        return json.loads(r.content)


    def delete_user(self, user_id):
        url = self.base_url + 'users/' + str(user_id)
        r = requests.delete(url, headers={
            'api-key': settings.BRIVO_API_KEY,
            'Authorization': f'bearer {self.access_token}',
        })

    def list_groups(self, name):
        url = self.base_url + 'groups?' + f'filter=name__eq:{self.brivo_group_prefix} {name}'

        r = requests.get(url,  headers={
            'api-key': settings.BRIVO_API_KEY,
            'Authorization': f'bearer {self.access_token}'})
        return json.loads(r.content)

    def retrieve_by_id(self, id):
        url = self.base_url + 'users/' + str(id)

        r = requests.get(url,  headers={
            'api-key': settings.BRIVO_API_KEY,
            'Authorization': f'bearer {self.access_token}'})
        print(r.content)
        return json.loads(r.content)

    def retrieve_user_groups(self, id):
        url = self.base_url + 'users/' + str(id) + '/groups'

        r = requests.get(url, headers={
            'api-key': settings.BRIVO_API_KEY,
            'Authorization': f'bearer {self.access_token}'})
        print(r.content)
        return json.loads(r.content)


    def assign_user_to_group(self, user_id, group_id):
        url = self.base_url + f'groups/{group_id}/users/{user_id}'
        r = requests.put(url, headers={
            'api-key': settings.BRIVO_API_KEY,
            'Authorization': f'bearer {self.access_token}'})
        if r.status_code in [404, 400]:
            print("The group or tenant not found. User didn't get access.")
        else:
            print("Tenant was assigned")

        return r

    def remove_user_from_group(self, user_id, group_id):
        url = self.base_url + f'groups/{group_id}/users/{user_id}/'
        r = requests.delete(url, headers={
            'api-key': settings.BRIVO_API_KEY,
            'Authorization': f'bearer {self.access_token}'})

        if r.status_code in [404, 400]:
            print("The group or tenant not found. User have access.")
        else:
            print("Tenant was removed from group")

        return r







