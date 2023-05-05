import requests
import os
import json
from elasticsearch import helpers
from datetime import datetime
from elasticsearch import Elasticsearch
from transformers import pipeline
from concurrent.futures import ThreadPoolExecutor
import glob
import time
import threading

sentiment_pipeline = pipeline("sentiment-analysis")

def save_tweets_locally(tweets, folder="buffered_tweets"):
    os.makedirs(folder, exist_ok=True)
    timestamp = int(time.time())
    filename = os.path.join(folder, f"tweets_{timestamp}.json")
    with open(filename, "w") as f:
        json.dump(tweets, f)

def load_tweets_from_local(folder="buffered_tweets"):
    tweet_files = glob.glob(os.path.join(folder, "*.json"))
    tweets = []
    for tweet_file in tweet_files:
        with open(tweet_file, "r") as f:
            tweets.extend(json.load(f))
        os.remove(tweet_file)
    return tweets

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
es_host = "localhost"
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
    print(json.dumps(response.json()))
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
    print(json.dumps(response.json()))

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
    print(json.dumps(response.json()))

def is_elasticsearch_up():
    try:
        return es.ping()
    except Exception as e:
        print(f"Error checking Elasticsearch status: {e}")
        return False


def index_local_tweets_periodically():
    while True:
        if is_elasticsearch_up():
            # Index local tweets saved in JSON files
            local_tweets = load_tweets_from_local()
            if local_tweets:
                bulk_index_tweets_in_elasticsearch(local_tweets)

            # Sleep for a certain interval before checking again
            time.sleep(60)
        else:
            # If Elasticsearch is down, wait for some time before checking its status again
            time.sleep(60)
def index_tweet_in_elasticsearch(tweet):
    max_retries = 1
    retries = 0
    indexed = False



    while not indexed and retries < max_retries:
        try:
            res = es.index(index="twitter", document=tweet)
            print(f"Indexed tweet in Elasticsearch: {res['result']}")
            indexed = True
        except Exception as e:
            print(f"Error indexing tweet in Elasticsearch: {e}")
            retries += 1
            time.sleep(2 * retries)
    return indexed

# def index_local_tweets_periodically():
#     while True:
#         if is_elasticsearch_up():
#             # Index local tweets saved in JSON files
#             local_tweets = load_tweets_from_local()
#             if local_tweets:
#                 bulk_index_tweets_in_elasticsearch(local_tweets)
#
#             # Sleep for a certain interval before checking again
#             time.sleep(60)
#         else:
#             # If Elasticsearch is down, wait for some time before checking its status again
#             time.sleep(60)

# Start a separate thread to handle indexing of local tweets periodically
index_local_thread = threading.Thread(target=index_local_tweets_periodically)
index_local_thread.start()


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
    print(f"Sentiment result: {sentiment_result}")

    json_response["sentiment_result"] = sentiment_result

    # Check if the "matching_rules" key exists in the json_response
    if "matching_rules" in json_response and json_response["matching_rules"]:
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

    return json_response

def get_stream():
    def index_buffered_tweets(processed_tweets):
        if is_elasticsearch_up():
            bulk_index_tweets_in_elasticsearch(processed_tweets)
        else:
            save_tweets_locally(processed_tweets)

    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream",
        auth=bearer_oauth,
        stream=True,
        params={"tweet.fields": "created_at,entities,in_reply_to_user_id", "expansions": "referenced_tweets.id"},
    )
    print(response.status_code)
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
                print(json.dumps(json_response, indent=4, sort_keys=True))

                processed_tweet = process_tweet(json_response)

                tweet_buffer.append(processed_tweet)

                if len(tweet_buffer) >= buffer_size:
                    index_buffered_tweets_thread = threading.Thread(target=index_buffered_tweets, args=(tweet_buffer,))
                    index_buffered_tweets_thread.start()
                    index_buffered_tweets_thread.join()

                    tweet_buffer = []



def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set_rules(delete)
    get_stream()

if __name__ == "__main__":
    main()

    index_local_thread = threading.Thread(target=index_local_tweets_periodically)
    index_local_thread.start()


