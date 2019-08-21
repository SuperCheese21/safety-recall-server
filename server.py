import logging
from time import sleep

import firebase_admin
from firebase_admin import credentials, firestore, messaging
from firebase_admin.messaging import ApiCallError


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

    # set test notification content
    notification = messaging.Notification(
        title='Test',
        body='This is a test notification'
    )

    # infinite loop that sends messages every 5 minutes
    while True:
        send_messages(android_config, collection, notification)
        sleep(300)


def send_messages(android_config, collection, notification):
    logging.info("Sending messages...")
    sent_messages = []

    # iterate over users in users collection
    for document in collection.stream():
        # get user's push token and build message object
        push_token = document.to_dict()['pushToken']
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
