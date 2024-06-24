import boto3
import os
import tempfile
import subprocess

def lambda_handler(event, context):
    # Initialize S3 clients
    s3 = boto3.client('s3')
    source_bucket = 'source-bucket-account-a-kpit'
    destination_bucket = 'destination-bucket-account-b-kpit'
    role_arn = 'arn:aws:iam::730335609507:role/service-role-b'
    
    for record in event['Records']:
        key = record['s3']['object']['key']
        
        # Check if the object is an .mf4 file
        if key.endswith('.mf4'):
            try:
                # Download the .mf4 file from the source bucket
                with tempfile.TemporaryDirectory() as temp_dir:
                    download_path = os.path.join(temp_dir, os.path.basename(key))
                    s3.download_file(source_bucket, key, download_path)
                    
                    # Convert the .mf4 file to .csv using the convert_fot.py script
                    output_path = os.path.join(temp_dir, key.replace('.mf4', '.csv'))
                    subprocess.run(
                        ['python', '/var/task/convert_fot.py', download_path, output_path],
                        check=True
                    )
                    
                    # Upload the .csv file back to the source bucket
                    csv_key = key.replace('.mf4', '.csv')
                    s3.upload_file(output_path, source_bucket, csv_key)
                    
            except Exception as e:
                print(f"Error during conversion: {e}")
                continue
        
        else:
            # If not .mf4 file, copy the object to the destination bucket
            copy_source = {'Bucket': source_bucket, 'Key': key}
            
            # Assume role in Account B
            sts_client = boto3.client('sts')
            assumed_role_object = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName="LambdaCrossAccountSession"
            )
            
            credentials = assumed_role_object['Credentials']
            
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
            )
            
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=destination_bucket,
                Key=key
            )

    return {'statusCode': 200, 'body': 'Success'}
