access_token = "2943335385-pAJ706hfIxJkRA9nkWPIvSiqroI61xjeZmeAoF1"
access_token_secret = "UjnaPulIIpBN6WAFYYuvJKcJ3diowxzrVQXkTIRg0Z9oK"
consumer_key = "Ky6lyW6xrjOkXJYEB0PxjUnGi"
consumer_secret = "vBKPwBE4SyvEatteJP1LPcRhhrnfRzb3HchqMYG2GpzqT53LpH"

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import re
import json
import threading

topic_name = "ece_2020_fall_app_2"


CEND    = '\33[0m'
CBLUE   = '\33[34m'
CYELLOW2 = '\33[93m'
CRED    = '\33[31m'
CGREEN2  = '\33[92m'

def print_info(*args):
    """
    Prints the string in green color
    :param args: user string information
    :return: stdout
    """
    print(CGREEN2 + str(*args) + CEND)

def print_error(*args):
    """
    Prints the string in red color
    :param args: user string information
    :return: stdout
    """
    print(CRED + str(*args) + CEND)

def print_warn(*args):
    """
    Prints the string in yellow color
    :param args: user string information
    :return: stdout
    """
    print(CYELLOW2 + str(*args) + CEND)

def print_debug(*args):
    """
    Prints the string in blue color
    :param args: user string information
    :return: stdout
    """
    print(CBLUE + str(*args) + CEND)

class AIKeyWords(object):
    AI = "#AI|Artificial Intelligence|robotics"
    ML = "machinelearningengineer|Machine Learning|scikit|#ML|mathematics"
    DL = "DeepLearning|Deep Learning|#DL|Tensorflow|Pytorch|Neural Network|NeuralNetwork"
    CV = "computervision|computer vision|machine vision|machinevision|convolutional network|convnet|image processing"
    NLP = "NLP|naturallanguageprocessing|natural language processing|text processing|text analytics|nltk|spacy"
    DATA = "iot|datasets|dataengineer|analytics|bigdata|big data|data science|data analytics|data insights|data mining|distributed computing|parallel processing|apache spark|hadoop|apache hive|airflow|mlflow|apache kafka|hdfs|apache|kafka"
    TWEET_HASH_TAGS = "dataanalysis|AugmentedIntelligence|datascience|machinelearning|rnd|businessintelligence|DigitalTransformation|datamanagement|ArtificialIntelligence"
    FALSE_POSITIVE = "gpu|nvidia|maths|mathematics|intelligence|conspiracy|astrology|vedic|tamil|text|computer|ebook|pdf|learning|big|insights|processing|network|machine|artifical|data|science|parallel|computing|deep|vision|natural|language|data"
    RANDOM_TOPICS = "nature|climate|space|earth|animals|plants|astrology|horoscope|occult|hidden science|conspiracy|hinduism|hindu|vedic"

    POSITIVE = AI + "|" + ML + "|" + DL + "|" + CV + "|" + NLP + "|" + DATA + "|" + TWEET_HASH_TAGS
    ALL = POSITIVE + "|" + FALSE_POSITIVE + "|" + RANDOM_TOPICS


def pick_text(text, rtext, etext):
    """
    Twitter Json data has three level of text. This function picks what is available in the order etext > rtext > text
    :param text: Plain text at top level of the Json with stipped content and an URL
    :param rtext: Retweeted full text
    :param etext: Extended retweeted full text
    :return:
    """
    ret = ""
    if etext:
        ret = etext
    elif rtext:
        ret = rtext
    elif text:
        ret = text
    else:
        ret = ""

    return re.sub("\n|\r", "", ret).strip()

class TweetsListener(StreamListener):
    """
    Tweepy StreamListener.
    Reference: http://docs.tweepy.org/en/latest/streaming_how_to.html
    
    :param is_ai: (bool) Used to differentiate AI tweets wuth green color and red for other category tweets
    """
    def __init__(self,
                 is_ai=False):


        StreamListener.__init__(self)
        self._is_ai = is_ai
        self._kafka_producer = KafkaProducer(bootstrap_servers=['kfk-brk-1.au.adaltas.cloud:6667','kfk-brk-2.au.adaltas.cloud:6667','kfk-brk-3.au.adaltas.cloud:6667'],security_protocol = 'SASL_PLAINTEXT',sasl_mechanism='GSSAPI')

    def on_data(self, data):
        """
        Gets triggered by the Twitter stream API
        :param data: Tweet Json data
        :return: dumps the data into screen
        """
        data_dict = json.loads(data)

        # Debug info
        if "text" in data_dict.keys():
            text = data_dict["text"]
        else:
            text = None

        if "extended_tweet" in data_dict.keys():
            etext = data_dict["extended_tweet"]["full_text"]
        else:
            etext = None

        if "retweeted_status" in data_dict.keys():
            if "extended_tweet" in data_dict["retweeted_status"].keys():
                rtext = data_dict["retweeted_status"]["extended_tweet"]["full_text"]
            else:
                rtext = None
        else:
            rtext = None

        text = pick_text(text=text, rtext=rtext, etext=etext)

        if self._is_ai:
            print_info(text)
        else:
            print_error(text)
        # with open("/tmp/tweets/{}.json".format(json.loads(data)["id_str"]), "wt", encoding='utf-8') as file:
        #     file.write(data)
        
        self._kafka_producer.send(topic_name, data.encode('utf-8')).get(timeout=10)
        return True

    def if_error(self, status):
        print(status)
        return True

class TwitterProducer(object):
    """
    Twitter data ingestion. Gets the twitter stream data.
    :param twitter_consumer_key: (str) Twitter Consumer Key
    :param twitter_consumer_secret: (str) Twitter Consumer secret
    :param twitter_access_token: (str)  Twitter Access token
    :param twitter_access_secret: (str) Twitter Access secret
    :param topic_1: (str) Tweet stream topic 
    :param topic_2: (str) Tweet stream topic
    :param topic_2_filter_words: (list) Filter words to be used for second stream
    """

    def __init__(self,
                 twitter_consumer_key,
                 twitter_consumer_secret,
                 twitter_access_token,
                 twitter_access_secret,
                 topic_1):

        self._twitter_consumer_key = twitter_consumer_key
        self._twitter_consumer_secret = twitter_consumer_secret
        self._twitter_access_token = twitter_access_token
        self._twitter_access_secret = twitter_access_secret

        self._topic_1 = topic_1
        
    def _twitter_stream(self, topic_name, keywords, is_ai=False):
        """
        :param topic_name:
        :param keywords:
        :param is_ai:
        :return:
        """
        auth = OAuthHandler(self._twitter_consumer_key, self._twitter_consumer_secret)
        auth.set_access_token(self._twitter_access_token, self._twitter_access_secret)

        print_info("\n\n---------------------------------------------------------------------------------\n\n")
        print_info(f"Topic name: {topic_name}")
        print_info(f"Twitter Keywords : {keywords}")
        print_info("\n\n---------------------------------------------------------------------------------\n\n")
        
        while True:
            try:
                twitter_stream = Stream(auth, TweetsListener(is_ai=is_ai))
                # https://developer.twitter.com/en/docs/tweets/filter-realtime/api-reference/post-statuses-filter
                # https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters
                twitter_stream.filter(track=keywords, languages=["en"])
            except Exception as e:
                print("Error: Restarting the twitter stream")

    def run(self):
        """
        """
        ai_stream = threading.Thread(target=self._twitter_stream, args=(self._topic_1, AIKeyWords.POSITIVE.split("|"), True,))
        ai_stream.setDaemon(True)
        ai_stream.start()
        ai_stream.join()

if __name__ == "__main__":
    producer = TwitterProducer(consumer_key,consumer_secret,access_token,access_token_secret,topic_name) # TODO add you credential details!
    producer.run()