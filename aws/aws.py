# -*- coding: utf-8 -*-
import re
import json
import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

class KeyManagementService:
    def __init__(self, env):
        self.secret_name = env.get_env_variable('SECRET_NAME')
        self.region_name = env.get_env_variable('REGION_NAME')
        self.client = boto3.session.Session().client(service_name='secretsmanager')

    def get_secret(self):
        try:
            get_secret_value_response = self.client.get_secret_value(SecretId=self.secret_name)
        except ClientError as e:
            raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

        return json.loads(secret)
    
class S3:
    def __init__(self, env):
        self.s3 = boto3.client('s3')
        self.prefix = env.get_env_variable('PREFIX')
        self.bucket_name = env.get_env_variable('BUCKET')

    def get_s3_folders(self, prefix):
        folders = []
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/')
        for page in pages:
            for prefix in page.get('CommonPrefixes', []):
                folders.append(prefix['Prefix'])
        return folders

    def get_s3_files(self, folder):
        results = []
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=folder)
        for page in pages:
            for obj in page.get('Contents', []):
                results.append(obj['Key'])
        return results

    def read_s3_file(self, file_key):
        contents = []

        response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
        message = response['Body'].read().decode('utf-8')
        lines = message.strip().split('\n')
            
        for line in lines:
            request = json.loads(line)
            payload = None
            content = None
                
            contents.append(content)

        df = pd.DataFrame(contents)
                
        return df
    
class Redshift:
    def __init__(self, env):
        self.env = env
        self.unpack_env_config(self.env)
        self.unpack_cloud_config()

    def unpack_cloud_config(self):
        self.kms = KeyManagementService(self.env)
        self.secret = self.kms.get_secret()

        self.username = self.secret['username']
        self.password = self.secret['password']
        self.port = self.secret['port']
        self.engine = self.secret['engine']
        self.host = self.secret['host']

    def unpack_env_config(self, env):
        self.database = env.get_env_variable('DATABASE')
        self.table = env.get_env_variable('TABLE')
        self.schema = env.get_env_variable('SCHEMA')
    
    def create_connection(self):
        connection_string = f"{self.engine}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.rs_engine = create_engine(connection_string, poolclass=NullPool)

        try:
            self.connection = self.rs_engine.connect()
        except exc.SQLAlchemyError as e:
            raise e

    def close_connection(self):
        if self.connection:
            self.connection.close()

    def send_prediction(self, post_df):
        self.create_connection()

        try:
            with self.connection.connect() as connection:
                post_df.to_sql(self.table, con=connection, schema=self.schema, if_exists='append', index=False, method='multi', chunksize=500)
        except exc.SQLAlchemyError as e:
            raise e
        finally:
            self.close_connection()