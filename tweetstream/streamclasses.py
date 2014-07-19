import time
import urllib
import urllib2
import socket
from platform import python_version_tuple
import anyjson
import oauth2

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
        self._conn = None
        self._rate_ts = None
        self._rate_cnt = 0
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._access_token = access_token
        self._access_secret = access_secret
        self._catchup_count = catchup
        self._raw_mode = raw
        self._timeout = timeout
        self._iter = self.__iter__()

        self.rate_period = 10  # in seconds
        self.connected = False
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

        postdata = self._get_post_data() or {}
        if self._catchup_count:
            postdata["count"] = self._catchup_count
        poststring = urllib.urlencode(postdata) if postdata else None

        headers = { 'User-Agent': self.user_agent,
                    'Authorization': self._get_oauth_header(postdata) }

        req = urllib2.Request(self.url, poststring, headers)

        # If connecting fails, convert to ReconnectExponentiallyError so
        # clients can implement appropriate backoff.
        try:
            self._conn = urllib2.urlopen(req, timeout=self._timeout)
        except urllib2.HTTPError, x:
            if x.code == 401:
                raise AuthenticationError(str(x))
            elif x.code == 404:
                raise ReconnectExponentiallyError("%s: %s" % (self.url, x))
            else:
                raise ReconnectExponentiallyError(str(x))
        except urllib2.URLError, x:
            raise ReconnectExponentiallyError(str(x))

        # This is horrible. This line grabs the raw socket (actually an ssl
        # wrapped socket) from the guts of urllib2/httplib. We want the raw
        # socket so we can bypass the buffering that those libs provide.
        # The buffering is reasonable when dealing with connections that
        # try to finish as soon as possible. With twitters' never ending
        # connections, it causes a bug where we would not deliver tweets
        # until the buffer was full. That's problematic for very low volume
        # filterstreams, since you might not see a tweet for minutes or hours
        # after they occured while the buffer fills.
        #
        # Oh, and the inards of the http libs are different things on in
        # py2 and 3, so need to deal with that. py3 libs do more of what I
        # want by default, but I wont do more special casing for it than
        # neccessary.

        major, _, _ = python_version_tuple()
        # The cast is needed because apparently some versions return strings
        # and some return ints.
        # On my ubuntu with stock 2.6 I get strings, which match the docs.
        # Someone reported the issue on 2.6.1 on macos, but that was
        # manually built, not the bundled one. Anyway, cast for safety.
        major = int(major)
        if major == 2:
            self._socket = self._conn.fp._sock.fp._sock
        else:
            self._socket = self._conn.fp.raw
            # our code that reads from the socket expects a method called recv.
            # py3 socket.SocketIO uses the name read, so alias it.
            self._socket.recv = self._socket.read

        self.connected = True
        if not self.starttime:
            self.starttime = time.time()
        if not self._rate_ts:
            self._rate_ts = time.time()

    def _get_oauth_header(self, postdata):
        """Return the content of the OAuth 1.0a Authorization: header."""
        # Note: This is a little weird in that we use a library (oauth2) that
        # is itself completely capable of making HTTP requests, but only to
        # generate the auth header. We do this, instead of adapting the above
        # to use oauth2 instead of urllib2, because oauth2 cannot (AFAICT)
        # open a persistent stream, which obviously we need.
        #
        # FIXME: This method is untested with non-empty postdata.
        conr = oauth2.Consumer(self._consumer_key, self._consumer_secret)
        token = oauth2.Token(self._access_token, self._access_secret)
        parms = { 'oauth_version': '1.0',
                  'oauth_nonce': oauth2.generate_nonce(),
                  'oauth_timestamp': str(int(time.time())) }
        method = 'GET'
        if (postdata):
            parms.update(postdata)
            method = 'POST'

        oauths = oauth2.Request(method=method, url=self.url, parameters=parms)
        # Setting is_form_encoded prevents 'oauth_body_hash', which causes
        # authentication failure, from being included in the OAuth parameters.
        # See <https://dev.twitter.com/discussions/4776>. (No, it doesn't make
        # any sense.)
        oauths.is_form_encoded = True
        oauths.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), conr, token)
        #pprint(oauths)

        # oauths2.Request.to_header() adds a bogus "realm" parameter, so we
        # mostly stringify ourselves, according to
        # <https://dev.twitter.com/docs/auth/authorizing-request>.
        return ('OAuth ' + ', '.join('%s="%s"' % (k, oauth2.escape(v))
                                     for (k, v) in oauths.iteritems()))

    def _get_post_data(self):
        """Subclasses that need to add post data to the request can override
        this method and return post data. The data should be in the format
        returned by urllib.urlencode."""
        return None

    def _update_rate(self):
        rate_time = time.time() - self._rate_ts
        if not self._rate_ts or rate_time > self.rate_period:
            self.rate = self._rate_cnt / rate_time
            self._rate_cnt = 0
            self._rate_ts = time.time()

    def __iter__(self):
        buf = b""
        while True:
            try:
                if not self.connected:
                    self._init_conn()

                buf += self._socket.recv(8192)
                if buf == b"":  # something is wrong
                    self.close()
                    raise ReconnectLinearlyError("Got entry of length 0. Disconnected")
                elif buf.isspace():
                    buf = b""
                elif b"\r" not in buf: # not enough data yet. Loop around
                    continue

                lines = buf.split(b"\r")
                buf = lines[-1]
                lines = lines[:-1]

                for line in lines:
                    if line != "\n":
                        if (self._raw_mode):
                            tweet = line
                        else:
                            try:
                                tweet = anyjson.deserialize(line)
                            except ValueError, e:
                                self.close()                        
                                raise ReconnectImmediatelyError("Got invalid data from twitter", details=line)

                        if 'text' in tweet:
                            self.count += 1
                            self._rate_cnt += 1
                        yield tweet


            except socket.error, e:
                self.close()
                raise ReconnectImmediatelyError("Server disconnected: %s" % (str(e)))


    def next(self):
        """Return the next available tweet. This call is blocking!"""
        return self._iter.next()


    def close(self):
        """
        Close the connection to the streaming server.
        """
        self.connected = False
        if self._conn:
            self._conn.close()


class SampleStream(BaseStream):
    url = "https://stream.twitter.com/1/statuses/sample.json"

class UserStream(BaseStream):
    url = "https://userstream.twitter.com/1.1/user.json"

class FilterStream(BaseStream):
    url = "https://stream.twitter.com/1/statuses/filter.json"

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
