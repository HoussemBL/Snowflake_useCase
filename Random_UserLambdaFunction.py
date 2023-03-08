import boto3
import urllib3
import random



def lambda_handler(event, context):
    
    http = urllib3.PoolManager()
    #Connect to S3
    s3 = boto3.client('s3')
    
    for x in range(1000):    
         # Make the API request
         response = http.request('GET', 'https://randomuser.me/api/?page=3&results=1000&seed=abc&inc=name,gender,nat,location')

         # Parse the JSON response
         data=response.data
         
         name_file = ''.join((random.choice('abcdxyzpqr') for i in range(5)))
         # Upload the JSON data to the S3 bucket
         s3.put_object(Bucket='houssem-snow-bucket', Key=name_file+'.json', Body=data)