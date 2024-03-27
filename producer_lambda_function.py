import json
import boto3
import random
import uuid
import datetime 

# create sqs client
sqs_client = boto3.client('sqs')

# specify sqs url
queue_url = "enter sqs queue url"

# generating mock data
city_country_pairs = [("Paris", "France"), ("Tokyo", "Japan"), ("New York", "USA"), ("London", "UK"), ("Sydney", "Australia"), ("Berlin", "Germany"),
                      ("Rome", "Italy"), ("Moscow", "Russia"), ("Beijing", "China"), ("Dubai", "UAE")]

def generate_booking_data():
    random_date = datetime.datetime(2024, random.randint(3, 12), random.randint(1, 30), random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    city, country = random.choice(city_country_pairs)
    location = "" + city + ", " + country

    return {
        "booking_id" : str(uuid.uuid1()),
        "user_id" : random.randint(1, 500),
        "property_id" : 'P' + str(random.randint(101, 400)),
        "location" : location,
        "start_date" : random_date.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date" : (random_date + datetime.timedelta(days=random.randint(0, 2),hours=random.randint(0, 23))).strftime("%Y-%m-%d %H:%M:%S"),
        "price" : random.randint(300, 1000)
    }

# lambda producer
def lambda_handler(event, context):
    i = 1

    while i <= 200:
        booking_data = generate_booking_data()
        print(booking_data)
        serialised_booking_data = json.dumps(booking_data)
        sqs_client.send_message(QueueUrl = queue_url, MessageBody = serialised_booking_data)

        i = i+1

    return {
        'statusCode': 200,
        'body': json.dumps('Booking Data Published to SQS !!!')
    }



