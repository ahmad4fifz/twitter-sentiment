import argparse
import json
import logging
import os

from dotenv import load_dotenv
from splunk_data_sender import SplunkSender
from textblob import TextBlob
from tweepy import API, OAuthHandler, Stream

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
        logger.info('Sentiment polarity: '+ tweet.sentiment.polarity)

        # determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        # output sentiment
        logger.info('Sentiment : '+ sentiment)

        # connect to Splunk
        

        # add text and sentiment info to elasticsearch
        es.index(index="sentiment",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"],
                       "polarity": tweet.sentiment.polarity,
                       "subjectivity": tweet.sentiment.subjectivity,
                       "sentiment": sentiment})
        return True

    # on failure
    def on_error(self, status):
        logging.error(status)


if __name__ == '__main__':

    # init display
    parser = argparse.ArgumentParser(description='Twitter sentiment analysis using Python and Splunk.')
    parser.add_argument('string', help='keyword(s) to query', nargs='+', action=includeSpacing)
    args = parser.parse_args()

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()
    logger.info('Creating a stream')

    # set twitter keys/tokens
    auth = OAuthHandler(os.getenv("TWITTER_CONSUMER_KEY"),
                        os.getenv("TWITTER_CONSUMER_SECRET"))
    auth.set_access_token(os.getenv("TWITTER_ACCESS_TOKEN"),
                          os.getenv("TWITTER_ACCESS_TOKEN_SECRET"))
    logger.info('Twitter keys and tokens loaded.')

    api = API(auth, wait_on_rate_limit=True)

    # try to authenticate with TwitterAPI
    try:
        api.verify_credentials()
        logger.info('Authentication sucess.')
    except Exception as e:
        logger.exception("Error during authentication:\n%s" % e)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)
    logger.info('Instance for Tweepy stream created.' + args.string)

    # search twitter for keyword supply
    logger.info('Query: ' + args.string)
    stream.filter(track=[args.string])
    