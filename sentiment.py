import json
import os

from dotenv import load_dotenv
from textblob import TextBlob
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener

# set the environment variables from .env file
load_dotenv()


class TweetStreamListener(StreamListener):

    # on success
    def on_data(self, data):

        # decode json
        dict_data = json.loads(data)

        # pass tweet into TextBlob
        tweet = TextBlob(dict_data["text"])

        # output sentiment polarity
        print(tweet.sentiment.polarity)

        # determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        # output sentiment
        print(sentiment)

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
        print(status)


if __name__ == '__main__':

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # set twitter keys/tokens
    auth = OAuthHandler(os.getenv("TWITTER_CONSUMER_KEY"),
                        os.getenv("TWITTER_CONSUMER_SECRET"))
    auth.set_access_token(os.getenv("TWITTER_ACCESS_TOKEN"),
                          os.getenv("TWITTER_ACCESS_TOKEN_SECRET"))

    # create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for "congress" keyword
    stream.filter(track=['congress'])
