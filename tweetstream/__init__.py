"""
Simple Twitter streaming API access
"""
__version__ = "0.2"
__author__ = "Rune Halvorsen <runefh@gmail.com>"
__homepage__ = "http://bitbucket.org/runeh/tweetstream/"
__docformat__ = "restructuredtext"

import urllib2
import time
import anyjson

SPRITZER_URL = "http://stream.twitter.com/spritzer.json"
USER_AGENT = "TweetStream %s" % __version__


class AuthenticationError(Exception):
    """Exception raised if the username/password is not accepted
    """
    pass


class ConnectionError(Exception):
    """Raised when there are network problems. This means when there are
    dns errors, network errors, twitter issues"""

    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return '<ConnectionError %s>' % self.reason


class TweetStream(object):
    """A network connection to Twitters streamign API

    :param username: Twitter username for the account accessing the API.
    :param password: Twitter password for the account accessing the API.

    :keyword url: URL to connect to. By default, the public "spritzer" url.

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

    def __init__(self, username, password, url=SPRITZER_URL):
        self._conn = None
        self._rate_ts = None
        self._rate_cnt = 0
        self._username = username
        self._password = password
        self._authenticated = False

        self.rate_period = 10 # in seconds
        self.url = url
        self.connected = False
        self.starttime = None
        self.count = 0
        self.rate = 0
        self.user_agent = USER_AGENT

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *params):
        if self._conn:
            self._conn.close()
        return False

    def _init_auth(self):
        """Set up authentication for the connection"""
        # stolen from the urllib2 missing manual
        # (http://www.voidspace.org.uk/python/articles/urllib2.shtmltutorial)
        try:
            password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
            top_level_url = "http://stream.twitter.com/"
            password_mgr.add_password(None, top_level_url, self._username,
                                      self._password)
            handler = urllib2.HTTPBasicAuthHandler(password_mgr)
            opener = urllib2.build_opener(handler)
            opener.open(self.url)
            urllib2.install_opener(opener)
        except urllib2.HTTPError, exception:
            if exception.code == 401:
                raise AuthenticationError("Access denied")
            else: # re raise. No idea what would cause this, so want to know
                raise
        except urllib2.URLError, exception:
            raise ConnectionError(exception.reason)

    def _init_conn(self):
        """Open the connection to the twitter server"""
        if not self._authenticated:
            self._init_auth()

        headers = {'User-Agent': self.user_agent}
        req = urllib2.Request(self.url, None, headers)
        self._conn = urllib2.urlopen(req)
        self.connected = True
        if not self.starttime:
            self.starttime = time.time()
        if not self._rate_ts:
            self._rate_ts = time.time()

    def next(self):
        """Return the next available tweet. This call is blocking!"""
        try:
            if not self.connected:
                self._init_conn()

            rate_time = time.time() - self._rate_ts
            if not self._rate_ts or rate_time > self.rate_period:
                self.rate = self._rate_cnt / rate_time
                self._rate_cnt = 0
                self._rate_ts = time.time()

            self.count += 1
            self._rate_cnt += 1
            data = self._conn.readline()
            if len(data) == 0: # something is wrong
                self.close()
                raise ConnectionError("Got entry of length 0. Disconnected")

            return anyjson.deserialize(data)
        except ValueError, e:
            self.close()
            raise ConnectionError("Got invalid data from twitter")

    def close(self):
        """
        Close the connection to the streaming server.
        """
        self.connected = False
        self._conn.close()


class ReconnectingTweetStream(TweetStream):
    """TweetStream class that automatically tries to reconnect if the
    connecting goes down. Reconnecting, and waiting for reconnecting, is
    blocking.

    :param username: See :TweetStream:

    :param password: See :TweetStream:

    :keyword url: See :TweetStream:

    :keyword reconnects: Number of reconnects before a ConnectionError is
        raised. Default is 3

    :error_cb: Optional callable that will be called just before trying to
        reconnect. The callback will be called with a single argument, the
        exception that caused the reconnect attempt. Default is None

    :retry_wait: Time to wait before reconnecting in seconds. Default is 5

    """

    def __init__(self, username, password, url=SPRITZER_URL,
                 reconnects=3, error_cb=None, retry_wait=5):
        self.max_reconnects = reconnects
        self.retry_wait = retry_wait
        self._reconnects = 0
        self._error_cb = error_cb
        TweetStream.__init__(self, username, password, url=url)

    def next(self):
        while True:
            try:
                return TweetStream.next(self)
            except ConnectionError, e:
                self._reconnects += 1
                if self._reconnects > self.max_reconnects:
                    raise ConnectionError("Too many retries")

                # Note: error_cb is not called on the last error since we
                # raise a ConnectionError instead
                if  callable(self._error_cb):
                    self._error_cb(e)

                time.sleep(self.retry_wait)
        # Don't listen to auth error, since we can't reasonably reconnect
        # when we get one.
