import json
import os
import sys
import uuid
from typing import Optional

import requests


class Auth(object):
    """
    example use:

        >>> from simperium.core import Auth
        >>> auth = Auth('myapp', 'cbbae31841ac4d44a93cd82081a5b74f')
        >>> Auth.create('john@company.com', 'secret123')
        'db3d2a64abf711e0b63012313d001a3b'
    """
    def __init__(self, appname: str, api_key: str, host: Optional[str]=None, scheme: str='https') -> None:
        """
        Inits the Auth class.
        """
        if not host:
            host = os.environ.get('SIMPERIUM_AUTHHOST', 'auth.simperium.com')
        self.appname = appname
        self.api_key = api_key
        self.host = host
        self.scheme = scheme

    def _build_url(self, endpoint: str) -> str:
        return '{}://{}/1/{}'.format(self.scheme, self.host, endpoint)

    def create(self, username: str, password: str) -> Optional[str]:
        """
        Create a new user with `username` and `password`.
        Returns the user access token if successful, or None otherwise.
        """

        data = {
            'client_id': self.api_key,
            'username': username,
            'password': password, }

        url = '{}://{}/1/{}'.format(self.scheme, self.host, self.appname+'/create/')
        url = self._build_url(self.appname+'/create/')
        try:
            r = requests.post(url, data=data)
            return r.json()['access_token']
        except ValueError:
            # invalid json
            return None
        except KeyError:
            # no access_token
            return None
        except Exception:
            # TODO: handle http errors
            return None

    def authorize(self, username: str, password: str) -> str:
        """
        Get the access token for a user.
        Returns the access token as a string or raises an error on failure.
        """
        data = {
            'client_id': self.api_key,
            'username': username,
            'password': password, }

        url = '{}://{}/1/{}'.format(self.scheme, self.host, self.appname+'/authorize/')
        r = requests.post(url, data=data)
        return r.json()['access_token']


class Bucket(object):
    """
    example use:

        >>> from simperium.core import Bucket
        >>> bucket = Bucket('myapp', 'db3d2a64abf711e0b63012313d001a3b', 'mybucket')
        >>> bucket.set('item2', {'age': 23})
        True
        >>> bucket.set('item2', {'age': 25})
        True
        >>> bucket.get('item2')
        {'age': 25}
        >>> bucket.get('item2', version=1)
        {'age': 23}
    """

    BATCH_DEFAULT_SIZE = 100

    def __init__(self, appname, auth_token, bucket,
            userid=None,
            host=None,
            scheme='https',
            clientid=None):

        if not host:
            host = os.environ.get('SIMPERIUM_APIHOST', 'api.simperium.com')

        self.userid = userid
        self.host = host
        self.scheme = scheme
        self.appname = appname
        self.bucket = bucket
        self.auth_token = auth_token
        if clientid:
            self.clientid = clientid
        else:
            self.clientid = 'py-%s' % uuid.uuid4().hex

    def _auth_header(self):
        headers = {'X-Simperium-Token': '%s' % self.auth_token}
        if self.userid:
            headers['X-Simperium-User'] = self.userid
        return headers

    def _gen_ccid(self) -> str:
        return uuid.uuid4().hex

    def _build_url(self, endpoint: str) -> str:
        return '{}://{}/1/{}'.format(self.scheme, self.host, endpoint)

    def index(self, data=False, mark=None, limit=None, since=None):
        """
        retrieve a page of the latest versions of a buckets documents
        ordered by most the most recently modified.

        @mark:    mark the documents returned to be modified after the
                  given cv
        @limit:   limit page size to this number.  max 1000, default 100.
        @since:   limit page to documents changed since the given cv.
        @data:    include the current data state of each  document in the
                  result. by default data is not included.

        returns: {
            'current':  head cv of the most recently modified document,
            'mark':     cv to use to pull the next page of documents. only
                        included in the repsonse if there are remaining pages
                        to fetch.
            'count':    the total count of documents available,

            'index': [{
                'id':  id of the document,
                'v:    current version of the document,
                'd':   optionally current data of the document, if
                       data is requested
                }, {....}],
            }
        """
        url = self._build_url('%s/%s/index' % (self.appname, self.bucket))

        args = {}
        if data:
            args['data'] = '1'
        if mark:
            args['mark'] = str(mark)
        if limit:
            args['limit'] = str(limit)
        if since:
            args['since'] = str(since)

        r = requests.get(url, headers=self._auth_header(), params=args)
        r.raise_for_status()
        return r.json()

    def get(self, item: str, default=None, version=None):
        """
        Retrieves either the latest version of item from this bucket, or the
        specific version requested.
        Returns `default` on a 404, raises error on http error
        """
        url = '%s/%s/i/%s' % (self.appname, self.bucket, item)
        if version:
            url += '/v/%s' % version
        url = self._build_url(url)

        r = requests.get(url, headers=self._auth_header())
        if r.status_code == 404:
            return default
        r.raise_for_status()

        return r.json()

    def post(self, item, data, version=None, ccid=None, include_response=False, replace=False):
        """posts the supplied data to item

            returns a unique change id on success, or None, if the post was not
            successful

            If `include_response` is True, returns a tuple of (`item`, the json
            response). Otherwise, returns `item`)
        """
        if not ccid:
            ccid = self._gen_ccid()

        url = '%s/%s/i/%s' % (self.appname, self.bucket, item)
        if version:
            url += '/v/%s' % version
        url = self._build_url(url)

        params = {
                'clientid': self.clientid,
                'ccid': ccid
                }

        if include_response:
            params['response'] = 1
        if replace:
            params['replace'] = 1

        try:
            r = requests.post(url, json=data, headers=self._auth_header(),
                    params=params)
            r.raise_for_status()
            # TODO: return none on http error
        except Exception as e:
            raise e
            return None
        if include_response:
            return item, r.json()
        else:
            return item

    def bulk_post(self, bulk_data, wait=True):
        """posts multiple items at once, bulk_data should be a map like:

            { "item1" : { data1 },
              "item2" : { data2 },
              ...
            }

            returns an array of change responses (check for error codes)
        """
        changes_list = []
        for itemid, data in list(bulk_data.items()):
            change = {
                "id"    : itemid,
                "o"     : "M",
                "v"     : {},
                "ccid"  : self._gen_ccid(),
            }
            # manually construct jsondiff, equivalent to jsondiff.diff( {}, data )
            for k, v in list(data.items()):
                change['v'][k] = {'o': '+', 'v': v}

            changes_list.append( change )

        url = '%s/%s/changes' % (self.appname, self.bucket)
        url = self._build_url(url)
        params = {
                'clientid': self.clientid
                }
        params['wait'] = 1

        r = request.post(url, data=changes_list, headers=self._auth_header(), params=params)
        r.raise_for_status()

        if not wait:
            # changes successfully submitted - check /changes
            return True

        # check each change response for 'error'
        return r.json()


    def new(self, data, ccid=None):
        return self.post(uuid.uuid4().hex, data, ccid=ccid)

    def set(self, item, data, **kw):
        return self.post(item, data, **kw)

    def delete(self, item, version=None):
        """deletes the item from bucket"""
        ccid = self._gen_ccid()
        url = '%s/%s/i/%s' % (self.appname, self.bucket, item)
        if version:
            url += '/v/%s' % version
        url = self._build_url(url)
        params = {
                'clientid': self.clientid,
                'ccid': ccid
                }
        r = requests.delete(url, headers=self._auth_header(),
                params=params)
        r.raise_for_status()
        if not r.text.strip():
            return ccid

    def changes(self, cv=None, timeout=None):
        """retrieves updates for this bucket for this user

            @cv: if supplied only updates that occurred after this
                change version are retrieved.

            @timeout: the call will wait for updates if not are immediately
                available.  by default it will wait indefinitely.  if a timeout
                is supplied an empty list will be return if no updates are made
                before the timeout is reached.
        """
        url = '%s/%s/changes' % (self.appname, self.bucket)
        url = self._build_url(url)
        params = {
                'clientid': self.clientid
                }
        if cv is not None:
            params['cv'] = cv
        headers = self._auth_header()
        try:
            r = requests.get(url, headers=headers, timeout=timeout,
                    params=params)
            r.raise_for_status()
        except http.client.BadStatusLine:
            # TODO: port this
            return []
        except Exception as e:
            if any(msg in str(e) for msg in ['timed out', 'Connection refused', 'Connection reset']) or \
                    getattr(e, 'code', None) in [502, 504]:
                return []
            raise
        return r.json()

    def all(self, cv=None, data=False, username=False, most_recent=False, timeout=None, skip_clientids=[], batch=None):
        """retrieves *all* updates for this bucket, regardless of the user
            which made the update.

            @cv: if supplied only updates that occurred after this
                change version are retrieved.

            @data: if True, also include the lastest version of the data for
                changed entity

            @username: if True, also include the username that created the
                change

            @most_recent: if True, then only the most recent change for each
                document in the current page will be returned. e.g. if a
                document has been recently changed 3 times, only the latest of
                those 3 changes will be returned.

            @timeout: the call will wait for updates if not are immediately
                available.  by default it will wait indefinitely.  if a timeout
                is supplied an empty list will be return if no updates are made
                before the timeout is reached.
        """
        url = '%s/%s/all' % ( self.appname, self.bucket)
        url = self._build_url(url)

        params = {
                'clientid': self.clientid,
                'cv': cv,
                'skip_clientid': skip_clientids
        }
        if username:
            params['username'] = 1
        if data:
            params['data'] = 1
        if most_recent:
            params['most_recent'] = 1
        try:
            params['batch'] = int(batch)
        except:
            params['batch'] = self.BATCH_DEFAULT_SIZE
        headers = self._auth_header()
        try:
            r = requests.get(url, headers=headers, timeout=timeout,
                    params=params)
        except http.client.BadStatusLine:
            # TODO: port to requests
            return []
        except Exception as e:
            # TODO: port to requests
            if any(msg in str(e) for msg in ['timed out', 'Connection refused', 'Connection reset']) or \
                    getattr(e, 'code', None) in [502, 504]:
                return []
            raise
        return r.json()


class SPUser(object):
    """
    example use:

        >>> from simperium.core import SPUser
        >>> user = SPUser('myapp', 'db3d2a64abf711e0b63012313d001a3b')
        >>> bucket.post({'age': 23})
        True
        >>> bucket.get()
        {'age': 23}
    """
    def __init__(self, appname, auth_token,
            host=None,
            scheme='https',
            clientid=None):

        self.bucket = Bucket(appname, auth_token, 'spuser',
            host=host,
            scheme=scheme,
            clientid=clientid)

    def get(self):
        return self.bucket.get('info')

    def post(self, data):
        self.bucket.post('info', data)


class Api(object):
    def __init__(self, appname, auth_token, **kw):
        self.appname = appname
        self.token = auth_token
        self._kw = kw

    def __getattr__(self, name):
        return Api.__getitem__(self, name)

    def __getitem__(self, name):
        if name.lower() == 'spuser':
            return SPUser(self.appname, self.token, **self._kw)
        return Bucket(self.appname, self.token, name, **self._kw)


class Admin(Api):
    def __init__(self, appname, admin_token, **kw):
        self.appname = appname
        self.token = admin_token
        self._kw = kw

    def as_user(self, userid):
        return Api(self.appname, self.token, userid=userid, **self._kw)
