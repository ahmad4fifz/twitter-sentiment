# Twitter-Sentiment

Twitter sentiment analysis using Python and Splunk.

Further information, please refer to my documentation [here](https://ahmad4fifz.github.io/docs/twitter-sentiment/).

## To-do:

- Fix this error:

```bash
(venv) ahmad@development:~/Documents/GitHub/twitter-sentiment$ python tweet-sentiment.py imran
Traceback (most recent call last):
  File "/home/ahmad/Documents/GitHub/twitter-sentiment/tweet-sentiment.py", line 138, in <module>
    listener = TweetStreamListener()
TypeError: __init__() missing 4 required positional arguments: 'consumer_key', 'consumer_secret', 'access_token', and 'access_token_secret'
```