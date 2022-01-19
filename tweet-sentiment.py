import argparse
import json
import logging
import os
import sys

from dotenv import load_dotenv
from splunk_data_sender import SplunkSender
from textblob import TextBlob
from tweepy import API, Client, OAuthHandler, Stream

# create a logger
logger = logging.getLogger(__name__)

# set formatter
console_formatter = logging.Formatter('%(levelname)s -- %(message)s')
file_formatter = logging.Formatter(
    '%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s')

# define handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_formatter)
file_handler = logging.FileHandler('app.log')
file_handler.setFormatter(file_formatter)

# add handler to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# set the environment variables from .env file
load_dotenv()
logger.info('Environment variables loaded from .env file')

consumer_key = os.getenv("TWITTER_CONSUMER_KEY")
consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")
access_token = os.getenv("TWITTER_ACCESS_TOKEN")
access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
bearer_token = os.getenv("TWITTER_BEARER_TOKEN")

splunk_conf = SplunkSender(
    endpoint=os.getenv("SPLUNK_ENDPOINT"),
    port=os.getenv("SPLUNK_PORT"),
    token=os.getenv("SPLUNK_HEC_TOKEN"),
    index='main',
    channel=os.getenv("SPLUNK_CHANNEL"),  # GUID
    api_version='0.1',
    hostname='tweet-sentiment',
    source='sentiment',
    source_type='_json',
    allow_overrides=True,
    verify=False,  # turn SSL verification on or off, defaults to True
    # timeout=60, # timeout for waiting on a 200 OK from Splunk server, defaults to 60s
    # retry_count=5, # Number of retry attempts on a failed/erroring connection, defaults to 5
    # retry_backoff=2.0,  # Backoff factor, default options will retry for 1 min, defaults to 2.0
    # turn on debug mode; prints module activity to stdout, defaults to False
    # enable_debug=True,
)


class includeSpacing(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, ' '.join(values))


class TweetStreamListener(Stream):

    # on success
    def on_data(self, data):

        # decode json
        dict_data = json.loads(data)

        # pass tweet into TextBlob
        tweet = TextBlob(dict_data["text"])
        logger.info('Tweet pass to TextBlob')

        # output sentiment polarity
        logger.info('Sentiment polarity: ' + tweet.sentiment.polarity)

        # determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        # output sentiment
        logger.info('Sentiment : ' + sentiment)

        # Splunk healthcheck
        is_alive = splunk.get_health()
        logging.info(is_alive)
        if not is_alive:
            logging.exception("Splunk HEC not alive")
            raise

        # add text and sentiment info to Splunk
        json_record = {  # this record will be parsed as normal text due to default "sourcetype" conf param
            "source": "sentiment",
            "host": "tweet-sentiment",
            "sourcetype": "_json",
            "index": "main",
            "event": {"author": dict_data["user"]["screen_name"],
                      "date": dict_data["created_at"],
                      "message": dict_data["text"],
                      "polarity": tweet.sentiment.polarity,
                      "subjectivity": tweet.sentiment.subjectivity,
                      "sentiment": sentiment
                      }
        }
        payloads = [json_record]

        splunk_res = splunk.send_data(payloads)
        logging.info(splunk_res)

        ack_id = splunk_res.get('ackId')
        splunk_ack_res = splunk.send_acks(ack_id)
        logging.info(splunk_ack_res)

        return True

    # on failure
    def on_error(self, status):
        logging.error(status)


if __name__ == '__main__':

    # init display
    parser = argparse.ArgumentParser(
        description='Twitter sentiment analysis using Python and Splunk.')
    parser.add_argument('string', help='keyword(s) to query',
                        nargs='+', action=includeSpacing)
    args = parser.parse_args()

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()
    logger.info('Creating a stream')

    # set twitter keys/tokens
    if bearer_token is None:
        # Twitter API v1.1 Interface
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = API(auth, wait_on_rate_limit=True)
        logger.info('Using 0Auth 1a authentication')
    else:
        # Twitter API v2 Client
        auth = Client(bearer_token)
        api = API(auth, wait_on_rate_limit=True)
        logger.info('Using 0Auth 2 authentication')

    # try to authenticate with TwitterAPI
    try:
        api.verify_credentials()
        logger.info('Authentication sucess.')
    except Exception as e:
        logger.exception("Error during authentication:\n%s" % e)
        sys.exit(1)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)
    logger.info('Instance for Tweepy stream created.')

    # pass Splunk conf
    splunk = SplunkSender(**splunk_conf)
    logger.info('Splunk conf passed')

    # search Twitter for keyword supply
    logger.info('Query: ' + args.string)
    stream.filter(track=[args.string])
