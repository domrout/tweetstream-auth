import time
import socket
import anyjson

import requests
from requests_oauthlib import OAuth1
from . import AuthenticationError, ConnectionError, USER_AGENT
from . import ReconnectImmediatelyError, ReconnectLinearlyError, ReconnectExponentiallyError

class BaseStream(object):
    """A network connection to Twitter's streaming API.

    :param consumer_key: OAuth consumer key for app accessing the API.
    :param consumer_secret: OAuth consumer secret for app.
    :param access_token: OAuth access token for user the app is acting on
      behalf of.
    :param access_secret: OAuth access token secret for user.

    :keyword count: Number of tweets from the past to get before switching to
      live stream.

    :keyword raw: If True, return each tweet's raw data direct from the socket,
      without UTF8 decoding or parsing, rather than a parsed object. The
      default is False.

    :keyword timeout: If non-None, set a timeout in seconds on the receiving
      socket. Certain types of network problems (e.g., disconnecting a VPN)
      can cause the connection to hang, leading to indefinite blocking that
      requires kill -9 to resolve. Setting a timeout leads to an orderly
      shutdown in these cases. The default is None (i.e., no timeout).

    :keyword url: Endpoint URL for the object. Note: you should not
      need to edit this. It's present to make testing easier.

    .. attribute:: connected

        True if the object is currently connected to the stream.

    .. attribute:: url

        The URL to which the object is connected

    .. attribute:: starttime

        The timestamp, in seconds since the epoch, the object connected to the
        streaming api.

    .. attribute:: count

        The number of tweets that have been returned by the object.

    .. attribute:: rate

        The rate at which tweets have been returned from the object as a
        float. see also :attr: `rate_period`.

    .. attribute:: rate_period

        The ammount of time to sample tweets to calculate tweet rate. By
        default 10 seconds. Changes to this attribute will not be reflected
        until the next time the rate is calculated. The rate of tweets vary
        with time of day etc. so it's usefull to set this to something
        sensible.

    .. attribute:: user_agent

        User agent string that will be included in the request. NOTE: This can
        not be changed after the connection has been made. This property must
        thus be set before accessing the iterator. The default is set in
        :attr: `USER_AGENT`.
    """

    def __init__(self, consumer_key, consumer_secret, access_token,
                 access_secret, catchup=None, raw=False, timeout=None,
                 url=None):
        self._rate_ts = None
        self._rate_cnt = 0
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._access_token = access_token
        self._access_secret = access_secret
        self._raw_mode = raw
        self._timeout = timeout
        self._iter = self.__iter__()

        self.rate_period = 10  # in seconds
        self.connected = False
        self.response = None
        self.starttime = None
        self.count = 0
        self.rate = 0
        self.user_agent = USER_AGENT
        if url: self.url = url

    def __enter__(self):
        return self

    def __exit__(self, *params):
        self.close()
        return False

    def _init_conn(self):
        """Open the connection to the twitter server"""

        auth = OAuth1(self._consumer_key, self._consumer_secret,
                  self._access_token, self._access_secret)

        # If connecting fails, convert to ReconnectExponentiallyError so
        # clients can implement appropriate backoff.
        try:
            self.response = requests.post(self.url, 
                stream=True,
                auth=auth,
                data=self._get_post_data()
                )
            if self.response.status_code == 401:
                raise AuthenticationError("Could not authenticate with Twitter")
            elif self.response.status_code == 404:
                raise ReconnectExponentiallyError("%s: %s" % (self.url, self.response))
        except requests.exceptions.HTTPError, x:           
            raise ReconnectExponentiallyError(str(x))

        self.connected = True
        if not self.starttime:
            self.starttime = time.time()
        if not self._rate_ts:
            self._rate_ts = time.time()


    def _get_post_data(self):
        """Subclasses that need to add post data to the request can override
        this method and return post data. ."""
        return None

    def _update_rate(self):
        rate_time = time.time() - self._rate_ts
        if not self._rate_ts or rate_time > self.rate_period:
            self.rate = self._rate_cnt / rate_time
            self._rate_cnt = 0
            self._rate_ts = time.time()

    def __iter__(self):
        while True:
            try:
                if not self.connected:
                    self._init_conn()

                for line in self.response.iter_lines():
                    if line:
                        if (self._raw_mode):
                            tweet = line
                        else:
                            try:
                                tweet = anyjson.deserialize(line)
                            except ValueError, e:
                                self.close()                        
                                raise ReconnectImmediatelyError("Invalid data: %s" % line)
                                
                        if 'text' in tweet:
                            self.count += 1
                            self._rate_cnt += 1
                        yield tweet


            except socket.error, e:
                raise ReconnectImmediatelyError("Server disconnected: %s" % (str(e)))
        
    def next(self):
        """Return the next available tweet. This call is blocking!"""
        return self._iter.next()


    def close(self):
        """
        Close the connection to the streaming server.
        """
        self.connected = False


class SampleStream(BaseStream):
    url = "https://stream.twitter.com/1/statuses/sample.json"

class UserStream(BaseStream):
    url = "https://userstream.twitter.com/1.1/user.json"

class FilterStream(BaseStream):
    url = "https://stream.twitter.com/1.1/statuses/filter.json"

    def __init__(self, consumer_key, consumer_secret, access_token,
                 access_secret, follow=None, locations=None, track=None,
                 catchup=None, raw=False, timeout=None, url=None):
        self._follow = follow
        self._locations = locations
        self._track = track
        # remove follow, locations, track
        BaseStream.__init__(self, consumer_key, consumer_secret, access_token,
                            access_secret, raw=raw, timeout=timeout, url=url)

    def _get_post_data(self):
        postdata = {}
        if self._follow: postdata["follow"] = ",".join([str(e) for e in self._follow])
        if self._locations: postdata["locations"] = ",".join(self._locations)
        if self._track: postdata["track"] = ",".join(self._track)
        return postdata
