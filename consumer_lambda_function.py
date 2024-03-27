import boto3
import pandas as pd
import json
import time

# create sqs client
sqs_client = boto3.client('sqs')

# create s3 client
s3_client = boto3.client('s3')
bucket_name = "airbnb-booking-processed-data"

# specify sqs URL
queue_url = "enter sqs queue url"

# lambda consumer
def lambda_handler(event, context):

    # Generate a dynamic file name using a timestamp
    current_timestamp = int(time.time())  # Get the current Unix timestamp
    file_name = f"airbnb-processed-data-{current_timestamp}.json"

    # Receive messages from SQS queue
    response = sqs_client.receive_message(QueueUrl = queue_url, MaxNumberOfMessages = 10, WaitTimeSeconds = 2)

    messages = response.get('Messages', [])
    print("Total messages received in the batch : ", len(messages))

    # initialise list to store filtered records
    filtered_records = []

    for message in messages:
        # Process message
        print("Processing message: ", message)

        message_body = json.loads(message['Body'])

        # filter the records where booking duration is more than 1 day
        booking_duration_days = ((pd.to_datetime(message_body['end_date'])) - (pd.to_datetime(message_body['start_date']))).days
        print("Booking duration days:", booking_duration_days)

        if booking_duration_days > 1:
            filtered_records.append(message_body)
            print("filtered_records", filtered_records)

        # Delete message from the queue after processing to prevent reprocessing
        receipt_handle = message['ReceiptHandle']
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print("Message deleted from the queue")

    # create dataframe for filtered records
    processed_data = pd.DataFrame(filtered_records).to_json(orient='records')

    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=processed_data)
    print(f"File '{file_name}' uploaded to bucket '{bucket_name}'")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed data written to S3 !!!')
    }
