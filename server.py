import json
import logging
from time import sleep

import firebase_admin
from firebase_admin import credentials, firestore, messaging
from firebase_admin.messaging import ApiCallError
import requests

OAUTH_ENDPOINT = 'https://identity.fortellis.io/oauth2/aus1p1ixy7YL8cMq02p7/v1/token'
RECALL_ENDPOINT = 'https://api.fortellis.io/v1/safety-recalls/'


def main():
    # set default logging level to allow info messages to show up in stdout
    logging.basicConfig(level=logging.INFO)

    # create credentials object from credentials file
    cred = credentials.Certificate("./service-account-file.json")

    # initialize firebase app with credentials and database url
    app = firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://fortellis-recall-tracker.firebaseio.com'
    })
    db_client = firestore.client(app=app)

    # get users collection reference
    collection = db_client.collection('users')

    # set Android notification priority
    android_config = messaging.AndroidConfig(
        priority='high'
    )

    # infinite loop that sends messages every 5 minutes
    while True:
        send_messages(android_config, collection)
        sleep(300)


def send_messages(android_config, collection):
    sent_messages = []

    logging.info("Requesting OAuth token...")
    with open("./auth.json", "r") as auth_data:
        auth = json.load(auth_data)

    oauth2_token = requests.post(OAUTH_ENDPOINT, data=auth).json()['access_token']

    logging.info("Sending messages...")

    # iterate over users in users collection
    for user_document in collection.stream():
        user = user_document.to_dict()

        # get user's push token and build message object
        push_token = user['pushToken']

        if 'vehicles' not in user:
            continue

        for vehicle in user['vehicles']:
            logging.info("Requesting recall data...")
            res = requests.get(RECALL_ENDPOINT + "?vin=" + vehicle['carVIN'], headers={
                'Subscription-Id': 'test',
                'Authorization': 'Bearer ' + oauth2_token
            }).json()

            for recall in res['items']:
                # set test notification content
                notification = messaging.Notification(
                    title="Safety Recall Alert - " + vehicle['year'] + " " + vehicle['make'] + " " + vehicle['model'],
                    body=recall['componentDescription']
                )

                # construct message
                message = messaging.Message(
                    notification=notification,
                    android=android_config,
                    token=push_token
                )

                # send message and catch errors
                try:
                    message_id = messaging.send(message)
                    sent_messages.append(message_id)
                except ApiCallError:
                    print("ApiCallError for push token " + push_token)

    logging.info(f"  Sent {len(sent_messages)} messages")


if __name__ == '__main__':
    main()
