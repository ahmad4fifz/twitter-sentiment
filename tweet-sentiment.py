import argparse
import json
import logging
import os
import sys
import malaya

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

# define log level
logger.setLevel("DEBUG")

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


# declare malaya 
corrector = malaya.spell.probability()
normalizer=malaya.normalize.normalizer(corrector)
transformer = malaya.translation.ms_en.transformer()


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
        

        # only take non-retweeted status
        if 'retweeted_status' not in dict_data:
            tweet = TextBlob(dict_data["text"])
            logger.info('Tweet pass to TextBlob')
            
            # check full tweet
            if 'extended_tweet' in dict_data:
                tweet=TextBlob(dict_data["extended_tweet"]["full_text"])
            
            # normalized text,translate from MS to EN
            normalized= normalizer.normalize(str (tweet))
            normalized_extract= normalized.get('normalize')
            translated=transformer.greedy_decoder([str(normalized_extract)])
            normalized_translated=''.join(translated)

            # add new KV with "text_translated" at the json 
            json_add= {"text_translated": normalized_translated}
            dict_data.update(json_add)

            # redeclare tweet with EN text for sentiment analysis
            tweet = TextBlob(dict_data["text_translated"])

            # output sentiment polarity
            logger.info('Sentiment polarity: ' + str(tweet.sentiment.polarity))

            # determine if sentiment is positive, negative, or neutral
            if tweet.sentiment.polarity < 0:
                sentiment = "negative"
            elif tweet.sentiment.polarity == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"

            # output sentiment
            logger.info('Sentiment : ' + sentiment)


            # add text and sentiment info to Splunk
            json_record = {  # this record will be parsed as normal text due to default "sourcetype" conf param
                "event": {"author": dict_data["user"]["screen_name"],
                        "date": dict_data["created_at"],
                        "message": dict_data["text"],
                        "translated_message": dict_data["text_translated"],
                        "polarity": tweet.sentiment.polarity,
                        "subjectivity": tweet.sentiment.subjectivity,
                        "sentiment": sentiment
                        }
            }

            
            payloads = [json_record]
            # payloads_json=json.dump(payloads)
            logging.info(payloads)
            
                # creating json file in output folder
            
            mode='a' if os.path.exists(filename)  else 'w'

            with open(filename,mode) as writing:
                
                # json.dump(json_record, writing, indent=4)
                json.dump(json_record, writing, indent=2)
                # writing.write(payloads_json)

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

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth, wait_on_rate_limit=True)
    logger.info('Using 0Auth 1a authentication')


    filename="output/"+args.string+".json"


    # try to authenticate with TwitterAPI
    try:
        api.verify_credentials()
        logger.info('Authentication sucess.')

        # create instance of the tweepy stream
        stream = TweetStreamListener(
            consumer_key, consumer_secret, access_token, access_token_secret)
        logger.info('Instance for Tweepy stream created.')

        # search Twitter for keyword supply
        logger.info('Query: ' + args.string)
        stream.filter(track=[args.string])

    except Exception as e:
        logger.exception("Error during authentication:\n%s" % e)
        sys.exit(1)
