#!/usr/bin/env python
#-*- coding: utf-8 -*-
import json, os
from sseclient import SSEClient as EventSource
from google.cloud import pubsub_v1

def create_subscription(topic_name):
    # define subscription name by topic name
    subscription_name = "projects/{project_id}/subscriptions/{subscription}".format(
        project_id=os.getenv("PROJECT_ID"),
        subscription=os.getenv("TOPIC_ID"),  # Set this to something appropriate.
    )

    # create a subscriber and subscription
    subscriber = pubsub_v1.SubscriberClient()
    subscriber.create_subscription(name=subscription_name, topic=topic_name)

# define topic name
topic_name = "projects/{project_id}/topics/{topic}".format(
    project_id=os.getenv("PROJECT_ID"),
    topic=os.getenv("TOPIC_ID"),  # Set this to something appropriate.
)

# create a publisher
publisher = pubsub_v1.PublisherClient()
project = publisher.project_path(os.getenv("PROJECT_ID"))

# list all topics by project
topics = []
for page in publisher.list_topics(project).pages:
    for topic in page:
        topics.append(topic.name)

# create a topic if not exists
if not topic_name in topics: 
    publisher.create_topic(topic_name)
    create_subscription(topic_name)

url = "https://stream.wikimedia.org/v2/stream/recentchange"
for event in EventSource(url):
    if event.event == "message":
        try:
            # check is a JSON valid
            change = json.loads(event.data)
        except ValueError as e:
            print(e)
        else:
            # print("User: {user} - edited: {title}".format(**change))
            response = publisher.publish(topic_name, bytes(json.dumps(change),"utf-8"))
            print(response.result())
