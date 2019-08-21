import logging
from time import sleep

import firebase_admin
from firebase_admin import credentials, firestore, messaging
from firebase_admin.messaging import ApiCallError


def main():
    logging.basicConfig(level=logging.INFO)
    cred = credentials.Certificate("./service-account-file.json")
    app = firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://fortellis-recall-tracker.firebaseio.com'
    })
    db_client = firestore.client(app=app)
    collection = db_client.collection('users')

    android_config = messaging.AndroidConfig(
        priority='high'
    )
    notification = messaging.Notification(
        title='Test',
        body='This is a test notification'
    )

    while True:
        send_messages(android_config, collection, notification)
        sleep(300)


def send_messages(android_config, collection, notification):
    logging.info("Sending messages...")
    sent_messages = []
    for document in collection.stream():
        push_token = document.to_dict()['pushToken']
        message = messaging.Message(
            notification=notification,
            android=android_config,
            token=push_token
        )
        try:
            message_id = messaging.send(message)
            sent_messages.append(message_id)
        except ApiCallError:
            print("ApiCallError for push token " + push_token)
    logging.info(f"  Sent {len(sent_messages)} messages")


if __name__ == '__main__':
    main()
