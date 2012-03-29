"""
Simple Twitter streaming API access
"""
__version__ = "0.3.5"
__author__ = "Rune Halvorsen <runefh@gmail.com>"
__homepage__ = "http://bitbucket.org/runeh/tweetstream/"
__docformat__ = "restructuredtext"

import urllib
import urllib2
import socket
import time
import anyjson
import logging

logger = logging.getLogger(__name__)

"""
 .. data:: URLS

     Mapping between twitter endpoint names and URLs.

 .. data:: USER_AGENT

     The default user agent string for stream objects

"""

URLS = {"firehose": "https://stream.twitter.com/1/statuses/firehose.json",
        "sample": "https://stream.twitter.com/1/statuses/sample.json",
        "follow": "https://stream.twitter.com/1/statuses/filter.json",
        "track": "https://stream.twitter.com/1/statuses/filter.json"}

USER_AGENT = "TweetStream %s" % __version__

socket._fileobject.default_bufsize = 0

class TweetStreamError(Exception):
    """Base class for all tweetstream errors"""
    pass

class AuthenticationError(TweetStreamError):
    """Exception raised if the username/password is not accepted
    """
    pass


class ConnectionError(TweetStreamError):
    """Raised when there are network problems. This means when there are
    dns errors, network errors, twitter issues"""

    def __init__(self, reason, details=None, exception=None):
        self.reason = reason
        self.details = details
        self.exception = exception

    def __str__(self):
        return '<ConnectionError %s>' % self.reason


class TweetStream(object):
    """A network connection to Twitter's streaming API

    :param username: Twitter username for the account accessing the API.
    :param password: Twitter password for the account accessing the API.

    :keyword url: URL to connect to. This can be either an endopoint name,
     such as "sample", or a full URL. By default, the public "sample" url
     is used. All known endpoints are defined in the :URLS: attribute

    .. attribute:: connected

        True if the object is currently connected to the stream.

    .. attribute:: url

        The URL to which the object is connected

    .. attribute:: want_json

        If True, the client will return raw JSON data rather than deserializing
        it into Python objects. Default False.

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

    def __init__(self, username, password, url="sample", want_json=False):
        self._conn = None
        self._rate_ts = None
        self._rate_cnt = 0
        self._username = username
        self._password = password
        self.want_json = want_json

        self.rate_period = 10 # in seconds
        self.connected = False
        self.starttime = None
        self.count = 0
        self.rate = 0
        self.user_agent = USER_AGENT
        self.url = URLS.get(url, url)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *params):
        self.close()
        return False

    def _init_conn(self):
        """Open the connection to the twitter server"""
        headers = {'User-Agent': self.user_agent}
        req = urllib2.Request(self.url, self._get_post_data(), headers)

        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, self.url, self._username,
                                  self._password)
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
        opener = urllib2.build_opener(handler)

        try:
            self._conn = opener.open(req)
        except urllib2.HTTPError, exception:
            logging.warning('error: %s', exception)
            if exception.code == 401:
                raise AuthenticationError("Access denied")
            elif exception.code == 404:
                raise ConnectionError("URL not found: %s" % self.url)
	    elif exception.code == 420:
		raise ConnectionError('Increase your calm')
            else: # re raise. No idea what would cause this, so want to know
                raise
        except urllib2.URLError, exception:
            raise ConnectionError(exception.reason, exception=exception)
            
        logger.info('connected')
        self.connected = True
        if not self.starttime:
            self.starttime = time.time()
        if not self._rate_ts:
            self._rate_ts = time.time()

    def _get_post_data(self):
        """Subclasses that need to add post data to the request can override
        this method and return post data. The data should be in the format
        returned by urllib.urlencode."""
        return None

    def next(self):
        """Return the next available tweet. This call is blocking!"""
        while True:
            try:
                if not self.connected:
                    self._init_conn()

                rate_time = time.time() - self._rate_ts
                if not self._rate_ts or rate_time > self.rate_period:
                    self.rate = self._rate_cnt / rate_time
                    self._rate_cnt = 0
                    self._rate_ts = time.time()

                data = self._conn.readline()
                if data == "": # something is wrong
                    self.close()
                    raise ConnectionError("Got entry of length 0. Disconnected")
                elif data.isspace():
                    continue

                self.count += 1
                self._rate_cnt += 1
                return data if self.want_json else anyjson.deserialize(data)

            except ValueError, e:
                self.close()
                raise ConnectionError("Got invalid data from twitter", details=data)

            except socket.error, e:
                self.close()
                raise ConnectionError("Server disconnected", exception=e)


    def close(self):
        """
        Close the connection to the streaming server.
        """
        self.connected = False
        if self._conn:
            self._conn.close()
            
    def _encode_all(self, l):
        return [i.encode('utf-8') for i in l]


class ReconnectingTweetStream(TweetStream):
    """TweetStream class that automatically tries to reconnect if the
    connecting goes down. Reconnecting, and waiting for reconnecting, is
    blocking.

    :param username: See :TweetStream:

    :param password: See :TweetStream:

    :keyword url: See :TweetStream:

    :keyword initial_wait: Number of seconds to wait after first error before
        trying to reconnect. This will be doubled after each failed reconnect
        attempt until a successful connection is made or max_wait is hit/exceeded.
        This is known as exponential backoff, see:
        http://dev.twitter.com/pages/streaming_api_concepts
        Default is 10

    :keyword max_wait: Max number of seconds to wait during exponential
        backoff before giving up and throwing a ConnectionError.

    :error_cb: Optional callable that will be called just before trying to
        reconnect. The callback will be called with a single argument, the
        exception that caused the reconnect attempt. Default is None

    """

    def __init__(self, username, password, url="sample", want_json=False,
                 initial_wait=10, max_wait=240, error_cb=None):
        self.initial_wait = initial_wait
        self.max_wait = max_wait
        self.curr_wait = initial_wait
        self._error_cb = error_cb
        TweetStream.__init__(self, username, password, url=url, want_json=want_json)

    def next(self):
        while True:
            try:
                tweet = TweetStream.next(self)
                # If we got here, no error was thrown, so reset curr_wait
                self.curr_wait = self.initial_wait
                return tweet
            except ConnectionError, e:
                if self.curr_wait >= self.max_wait:
                    logger.critical('giving up')
                    msg = 'max_wait (%d secs) exceeded, giving up on exponential backoff' % self.max_wait
                    raise ConnectionError(msg)
                # Note: error_cb is not called on the last error since we
                # raise a ConnectionError instead
                if  callable(self._error_cb):
                    self._error_cb(e)
                logger.warning('waiting %d seconds: %s %s', self.curr_wait, e, repr(e.exception))
                time.sleep(self.curr_wait)

                # Double curr_wait for next attempt
                self.curr_wait *= 2
                
        # Don't listen to auth error, since we can't reasonably reconnect
        # when we get one.

class FollowStream(TweetStream):
    """Stream class for getting tweets from followers.

    :param user: See TweetStream

    :param password: See TweetStream

    :param followees: Iterable containing user IDs to follow.
      ***Note:*** the user id in question is the numeric ID twitter uses,
      not the normal username.

    :keyword url: Like the url argument to TweetStream, except default
      value is the "follow" endpoint.
    """

    def __init__(self, user, password, followees, url="follow", **kwargs):
        self.followees = followees
        TweetStream.__init__(self, user, password, url=url, **kwargs)

    def _get_post_data(self):
        return urllib.urlencode({"follow": ",".join(map(str, self._encode_all(self.followees)))})


class TrackStream(TweetStream):
    """Stream class for getting tweets relevant to keywords.

        :param user: See TweetStream

        :param password: See TweetStream

        :param keywords: Iterable containing keywords to look for

        :keyword url: Like the url argument to TweetStream, except default
          value is the "track" endpoint.
    """

    def __init__(self, user, password, keywords, url="track", **kwargs):
        self.keywords = keywords
        TweetStream.__init__(self, user, password, url=url, **kwargs)

    def _get_post_data(self):
        return urllib.urlencode({"track": ",".join(self._encode_all(self.keywords))})


class ReconnectingTrackStream(ReconnectingTweetStream):
    """Stream class for getting tweets relevant to keywords, which
    automatically tries to reconnect if the connecting goes down. Reconnecting,
    and waiting for reconnecting, is blocking.

    :param user: See TweetStream

    :param password: See TweetStream

    :param keywords: Iterable containing keywords to look for

    :keyword url: Like the url argument to TweetStream, except default
      value is the "track" endpoint.

    :keyword initial_wait: see ReconnectingTweetStream

    :keyword max_wait: see ReconnectingTweetStream

    :error_cb: see ReconnectingTweetStream

    """

    def __init__(self, user, password, keywords, url="track", want_json=False,
            initial_wait=10, max_wait=240, error_cb=None):
        self.keywords = keywords
        ReconnectingTweetStream.__init__(self, user, password, url=url, want_json=want_json,
                initial_wait=initial_wait, max_wait=max_wait, error_cb=error_cb)

    def _get_post_data(self):
        return urllib.urlencode({"track": ",".join(self._encode_all(self.keywords))})


class ReconnectingTrackFollowStream(ReconnectingTweetStream):
    """Stream class for getting tweets relevant to keywords and following specific users, which
    automatically tries to reconnect if the connecting goes down. Reconnecting,
    and waiting for reconnecting, is blocking.

    :param user: See TweetStream

    :param password: See TweetStream

    :param keywords: Iterable containing keywords to look for

    :keyword url: Like the url argument to TweetStream, except default
      value is the "track" endpoint.

    :keyword initial_wait: see ReconnectingTweetStream

    :keyword max_wait: see ReconnectingTweetStream

    :error_cb: see ReconnectingTweetStream

    """

    def __init__(self, user, password, keywords, user_ids, url="track", want_json=False,
            initial_wait=10, max_wait=240, error_cb=None):
        self.keywords = keywords
        self.user_ids = user_ids
        ReconnectingTweetStream.__init__(self, user, password, url=url, want_json=want_json,
                initial_wait=initial_wait, max_wait=max_wait, error_cb=error_cb)

    def _get_post_data(self):
        data = dict()
        if len(self.keywords) > 0:
            data['track'] = ",".join(self._encode_all(self.keywords))
        if len(self.user_ids) > 0:
            data['follow'] = ",".join(self._encode_all(self.user_ids))
        return urllib.urlencode(data)
