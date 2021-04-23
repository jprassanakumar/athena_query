import boto3
import pandas as pd
import io
import re
import time
from datetime import datetime
import smart_open
BUCKET_NAME = '<S3_BUCKET_NAME>
notify_email = 'demo@github.com'
today_date = str(datetime.utcnow().year)+"-"+str(datetime.utcnow().month)+"-"+str(datetime.utcnow().day)
hour = str(datetime.utcnow().hour-1)
print("select count(*) count from cloudfront_logs WHERE 'date' BETWEEN DATE '"+today_date+"' AND DATE '"+today_date+"' and split_part(time,':',1)="+hour+" and status=301")
params = {
    'region': 'us-east-1',
    'database': 'default',
    'bucket': BUCKET_NAME,
    'path': 'temp/athena/output',
    'query': "select count(*) count from cloudfront_logs WHERE \"date\" BETWEEN DATE '"+today_date+"' AND DATE '"+today_date+"' and split_part(time,':',1)='"+hour+"' and uri like '%.js%' and status=301"
}

def athena_query(client, params):
    
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response

def athena_to_s3(session, params, max_execution = 50):
    client = session.client('athena', region_name=params["region"])
    execution = athena_query(client, params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    while (max_execution > 0 and state in ['RUNNING']):
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId = execution_id)

        if 'QueryExecution' in response and \
                'Status' in response['QueryExecution'] and \
                'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                return False
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]
                return filename
        time.sleep(1)
    
    return False


def cleanup(session, params):
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(params['bucket'])
    for item in my_bucket.objects.filter(Prefix=params['path']):
        item.delete()

session = boto3.Session()

# Query Athena and get the s3 filename as a result
s3_filename = athena_to_s3(session, params)
print(s3_filename)
s3 = boto3.resource('s3')
bucket = s3.Bucket(BUCKET_NAME)
obj = s3.Object(BUCKET_NAME, 'temp/athena/output/'+s3_filename)
print(obj)
body = obj.get()['Body'].read()
i=0
for line in smart_open.smart_open('s3://'+BUCKET_NAME+'/temp/athena/output/'+s3_filename):
    if i==0:
        i=i+1
        continue
    print(line.decode("utf-8").strip('\n'))
    if line.decode("utf-8").strip('\n')=="\"0\"":
        print("true")
    else:
        print("false")
        client = session.client('ses', region_name=params["region"])
        response = client.send_email(
    Destination={
        'ToAddresses': [notify_email],
    },
    Message={
        'Body': {
            'Text': {
                'Charset': 'UTF-8',
                'Data': 'count - '+line.decode("utf-8").strip('\n'),
            },
        },
        'Subject': {
            'Charset': 'UTF-8',
            'Data': '301 detected in cloudfront',
        },
    },
    Source=notify_email,
)
    i=i+1


# Removes all files from the s3 folder you specified, so be careful
cleanup(session, params)
