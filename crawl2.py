import requests
import os
import json
from elasticsearch import helpers
from datetime import datetime
from elasticsearch import Elasticsearch
from transformers import pipeline
from concurrent.futures import ThreadPoolExecutor
from functools import partial

sentiment_pipeline = pipeline("sentiment-analysis")

def bulk_index_tweets_in_elasticsearch(tweets):
    actions = [
        {
            "_index": "twitter",
            "_source": tweet,
        }
        for tweet in tweets
    ]
    helpers.bulk(es, actions)


def analyze_sentiment(text):
    result = sentiment_pipeline(text)[0]
    return result

bearer_token = "AAAAAAAAAAAAAAAAAAAAABu2QgEAAAAAW2YRXJEvoxQQm4df3%2Fus93KvwBg%3DiO6aRlc50ZJu6vsr1aQAGigHL6UvqfeWccjEN1paQBHqp0T69n"
es_host = "192.168.178.38"
es_port = "9200"

es = Elasticsearch([{"host": es_host, "port": 9200, "scheme": "http"}])

def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r

def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    # print(json.dumps(response.json()))
    return response.json()

def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    # print(json.dumps(response.json()))

def set_rules(delete):
    mitsotakis_and_tsipras_rules = [
            {"value": "mitsotakis OR μητσοτακης OR μητσοτάκης OR Μητσοτάκης OR Μητσοτάκης OR Κούλης OR Mitsotakis OR #Mitsotakis OR #mitsotakis OR #Μητσοτάκης", "tag": "mitsotakis"},
            {"value": "tsipras OR τσιπρας OR τσίπρας OR Τσίπρας OR Τσίπρας OR Αλέξης OR Tsipras OR #tsipras OR #Τσίπρας", "tag": "tsipras"}
        ]
    payload = {"add": mitsotakis_and_tsipras_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    # print(json.dumps(response.json()))

def index_tweet_in_elasticsearch(tweet):
    res = es.index(index="twitter", document=tweet)
    print(f"Indexed tweet in Elasticsearch: {res['result']}")

def process_tweet(json_response):
    tweet_data = json_response.get("data", {})
    tweet_text = tweet_data.get("text", "")

    if tweet_text.startswith("RT @"):
        # This is a retweet, so we need to get the full text from the original tweet
        referenced_tweets = json_response.get("includes", {}).get("tweets", [])
        if referenced_tweets:
            original_tweet = referenced_tweets[0]
            tweet_text = original_tweet.get("text", "")

    sentiment_result = analyze_sentiment(tweet_text)
    # print(f"Sentiment result: {sentiment_result}")

    json_response["sentiment_result"] = sentiment_result

    matched_rule_tag = json_response["matching_rules"][0]["tag"]
    if matched_rule_tag == "mitsotakis":
        json_response["tag"] = "Μητσοτάκης"
    elif matched_rule_tag == "tsipras":
        json_response["tag"] = "Τσίπρας"

    hashtags = []
    if "entities" in tweet_data and "hashtags" in tweet_data["entities"]:
        hashtags = [hashtag["tag"] for hashtag in tweet_data["entities"]["hashtags"]]

    # Add the hashtags to the tweet data
    tweet_data["hashtags"] = hashtags

    created_at = tweet_data["created_at"]
    created_at_datetime = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
    formatted_created_at = created_at_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    tweet_data["created_at"] = formatted_created_at

    # Update the text field of the tweet with the full text from the includes
    if tweet_text.startswith("RT @"):
        tweet_data["text"] = tweet_text
    elif "includes" in json_response and "tweets" in json_response["includes"] and len(json_response["includes"]["tweets"]) > 0:
        tweet_data["text"] = json_response["includes"]["tweets"][0]["text"]
    else:
        tweet_data["text"] = tweet_text

    # Check if the tweet is a reply, retweet or tweet
    if "referenced_tweets" in tweet_data:
        ref_tweet_type = tweet_data["referenced_tweets"][0]["type"]
        if ref_tweet_type == "retweeted":
            tweet_type = "retweet"
        elif ref_tweet_type == "replied_to":
            tweet_type = "reply"
        else:
            tweet_type = "tweet"
    elif "in_reply_to_user_id" in tweet_data:
        tweet_type = "reply"
    else:
        tweet_type = "tweet"
    tweet_data["tweet_type"] = tweet_type

    return json_response




def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream",
        auth=bearer_oauth,
        stream=True,
        params={"tweet.fields": "created_at,entities,in_reply_to_user_id", "expansions": "referenced_tweets.id"},
    )
    # print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )

    tweet_buffer = []
    buffer_size = 100

    with ThreadPoolExecutor(max_workers=5) as executor:
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                # print(json.dumps(json_response, indent=4, sort_keys=True))

                # Process and index reply tweets immediately
                if json_response.get("data", {}).get("in_reply_to_user_id"):
                    processed_tweet = process_tweet(json_response)
                    index_tweet_in_elasticsearch(processed_tweet)
                else:
                    tweet_buffer.append(json_response)

                if len(tweet_buffer) >= buffer_size:
                    processed_tweets = list(executor.map(process_tweet, tweet_buffer))
                    bulk_index_tweets_in_elasticsearch(processed_tweets)
                    tweet_buffer = []


def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set)

if __name__ == "__main__":
    main()