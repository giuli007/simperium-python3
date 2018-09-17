import os
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import requests


class Auth(object):
    """
    example use:

        >>> from simperium.core import Auth
        >>> auth = Auth('myapp', 'cbbae31841ac4d44a93cd82081a5b74f')
        >>> Auth.create('john@company.com', 'secret123')
        'db3d2a64abf711e0b63012313d001a3b'
    """

    def __init__(
        self,
        appname: str,
        api_key: str,
        host: Optional[str] = None,
        scheme: str = "https",
    ) -> None:
        """
        Inits the Auth class.
        """
        if not host:
            host = os.environ.get("SIMPERIUM_AUTHHOST", "auth.simperium.com")
        self.appname = appname
        self.api_key = api_key
        self.host = host
        self.scheme = scheme

    def _build_url(self, endpoint: str) -> str:
        return "{}://{}/1/{}".format(self.scheme, self.host, endpoint)

    def create(self, username: str, password: str) -> Optional[str]:
        """
        Create a new user with `username` and `password`.
        Returns the user access token if successful, or raises an error
        otherwise.
        """

        data = {"client_id": self.api_key, "username": username, "password": password}

        url = self._build_url(self.appname + "/create/")
        r = requests.post(url, data=data)
        r.raise_for_status()
        return r.json().get("access_token")

    def authorize(self, username: str, password: str) -> str:
        """
        Get the access token for a user.
        Returns the access token as a string or raises an error on failure.
        """
        data = {"client_id": self.api_key, "username": username, "password": password}
        url = self._build_url(self.appname + "/authorize/")
        r = requests.post(url, data=data)
        r.raise_for_status()
        return r.json()["access_token"]


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

    def __init__(
        self,
        appname: str,
        auth_token: str,
        bucket: str,
        userid: Optional[str] = None,
        host: Optional[str] = None,
        scheme: str = "https",
        clientid: Optional[str] = None,
    ) -> None:

        if not host:
            host = os.environ.get("SIMPERIUM_APIHOST", "api.simperium.com")

        self.userid = userid
        self.host = host
        self.scheme = scheme
        self.appname = appname
        self.bucket = bucket
        self.auth_token = auth_token
        if clientid:
            self.clientid = clientid
        else:
            self.clientid = "py-%s" % uuid.uuid4().hex

    def _auth_header(self) -> Dict[str, str]:
        headers = {"X-Simperium-Token": "%s" % self.auth_token}
        if self.userid:
            headers["X-Simperium-User"] = self.userid
        return headers

    def _gen_ccid(self) -> str:
        return uuid.uuid4().hex

    def _build_url(self, endpoint: str) -> str:
        return "{}://{}/1/{}".format(self.scheme, self.host, endpoint)

    def index(
        self,
        data: bool = False,
        mark: Optional[str] = None,
        limit: Optional[int] = None,
        since: Optional[str] = None,
    ) -> Dict[Any, Any]:
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
        url = self._build_url("%s/%s/index" % (self.appname, self.bucket))

        params = {
            "mark": str(mark) if mark is not None else None,
            "limit": str(limit) if limit is not None else None,
            "since": str(since) if since is not None else None,
            "data": "1" if data else None,
        }

        r = requests.get(  # type: ignore
            url, headers=self._auth_header(), params=params
        )
        r.raise_for_status()
        return r.json()

    def get(
        self, item: str, default: Any = None, version: Optional[int] = None
    ) -> Union[Any, Dict[Any, Any]]:
        """
        Retrieves either the latest version of item from this bucket, or the
        specific version requested.
        Returns `default` on a 404, raises error on http error

        `version` should be an integer > 0
        """
        url = "%s/%s/i/%s" % (self.appname, self.bucket, item)
        if version is not None:
            url += "/v/%s" % version
        url = self._build_url(url)

        r = requests.get(url, headers=self._auth_header())
        if r.status_code == 404:
            return default
        r.raise_for_status()

        return r.json()

    def post(
        self,
        item: str,
        data: Dict[Any, Any],
        version: Optional[int] = None,
        ccid: Optional[str] = None,
        include_response: bool = False,
        replace: bool = False,
    ) -> Optional[Union[str, Tuple[str, Dict[Any, Any]]]]:
        """
        Posts the supplied data to `item`.

        If `include_response` is True, returns a tuple of (`item`, the json
        response). Otherwise, returns `item`)

        `version` should be an integer > 0
        """
        ccid = ccid if ccid else self._gen_ccid()

        url = "%s/%s/i/%s" % (self.appname, self.bucket, item)
        if version is not None:
            url += "/v/%s" % version
        url = self._build_url(url)

        params = {
            "clientid": self.clientid,
            "ccid": ccid,
            "response": 1 if include_response else None,
            "replace": 1 if replace else None,
        }

        r = requests.post(url, json=data, headers=self._auth_header(), params=params)
        r.raise_for_status()
        if include_response:
            return item, r.json()
        else:
            return item

    def bulk_post(
        self, bulk_data: Dict[Any, Any], wait: bool = True
    ) -> Union[bool, Dict[Any, Any]]:
        """posts multiple items at once, bulk_data should be a map like:

            { "item1" : { data1 },
              "item2" : { data2 },
              ...
            }

            returns an array of change responses (check for error codes)
        """
        changes_list = []
        for itemid, data in list(bulk_data.items()):
            change = {"id": itemid, "o": "M", "v": {}, "ccid": self._gen_ccid()}
            # manually construct jsondiff, equivalent to jsondiff.diff( {}, data )
            for k, v in list(data.items()):
                change["v"][k] = {"o": "+", "v": v}

            changes_list.append(change)

        url = "%s/%s/changes" % (self.appname, self.bucket)
        url = self._build_url(url)
        params = {"clientid": self.clientid}
        params["wait"] = "1"

        r = requests.post(
            url, json=changes_list, headers=self._auth_header(), params=params
        )
        r.raise_for_status()

        if not wait:
            # changes successfully submitted - check /changes
            return True

        # check each change response for 'error'
        return r.json()

    def new(
        self,
        data: Dict[Any, Any],
        ccid: Optional[str] = None,
        include_response: bool = False,
    ) -> Optional[Union[str, Tuple[str, Dict[Any, Any]]]]:
        return self.post(
            uuid.uuid4().hex, data, ccid=ccid, include_response=include_response
        )

    def set(
        self,
        item: str,
        data: Dict[Any, Any],
        version: Optional[int] = None,
        ccid: Optional[str] = None,
        include_response: bool = False,
        replace: bool = False,
    ) -> Optional[Union[str, Tuple[str, Dict[Any, Any]]]]:
        return self.post(
            item,
            data,
            version=version,
            ccid=ccid,
            include_response=include_response,
            replace=replace,
        )

    def delete(self, item: str, version: Optional[int] = None) -> Optional[str]:
        """Deletes the item from bucket.
        Returns the ccid if the response is not an empty string.

        `version` should be an integer > 0
        """
        ccid = self._gen_ccid()
        url = "%s/%s/i/%s" % (self.appname, self.bucket, item)
        if version is not None:
            url += "/v/%s" % version
        url = self._build_url(url)
        params = {"clientid": self.clientid, "ccid": ccid}
        r = requests.delete(url, headers=self._auth_header(), params=params)
        r.raise_for_status()
        if not r.text.strip():
            return ccid
        return None

    def changes(self, cv=None, timeout=None):
        """retrieves updates for this bucket for this user

            @cv: if supplied only updates that occurred after this
                change version are retrieved.

            @timeout: the call will wait for updates if not are immediately
                available.  by default it will wait indefinitely.  if a timeout
                is supplied an empty list will be return if no updates are made
                before the timeout is reached.
        """
        url = "%s/%s/changes" % (self.appname, self.bucket)
        url = self._build_url(url)
        params = {"clientid": self.clientid}
        if cv is not None:
            params["cv"] = cv
        headers = self._auth_header()
        r = requests.get(url, headers=headers, timeout=timeout, params=params)
        r.raise_for_status()
        return r.json()

    def all(
        self,
        cv: Optional[str] = None,
        data: bool = False,
        username: bool = False,
        most_recent: bool = False,
        timeout: Optional[int] = None,
        skip_clientids: List[str] = [],
        batch: Optional[int] = None,
    ) -> Union[List[Any], Dict[Any, Any]]:
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
        url = "%s/%s/all" % (self.appname, self.bucket)
        url = self._build_url(url)

        params = {
            "clientid": self.clientid,
            "cv": cv,
            "skip_clientid": skip_clientids,
            "batch": str(batch) if batch is not None else str(self.BATCH_DEFAULT_SIZE),
            "username": "1" if username else None,
            "data": "1" if data else None,
            "most_recent": "1" if most_recent else None,
        }
        headers = self._auth_header()
        r = requests.get(  # type: ignore
            url, headers=headers, timeout=timeout, params=params
        )
        r.raise_for_status()
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

    def __init__(
        self,
        appname: str,
        auth_token: str,
        host: Optional[str] = None,
        scheme: str = "https",
        clientid: Optional[str] = None,
    ) -> None:

        self.bucket = Bucket(
            appname, auth_token, "spuser", host=host, scheme=scheme, clientid=clientid
        )

    def get(self) -> Dict[Any, Any]:
        return self.bucket.get("info")

    def post(
        self, data: Dict[Any, Any]
    ) -> Optional[Union[str, Tuple[str, Dict[Any, Any]]]]:
        return self.bucket.post("info", data)


class Api(object):
    def __init__(
        self,
        appname: str,
        auth_token: str,
        userid: Optional[str] = None,
        host: Optional[str] = None,
        scheme: str = "https",
        clientid: Optional[str] = None,
    ) -> None:
        self.appname = appname
        self.token = auth_token
        self.userid = userid
        self.host = host
        self.scheme = scheme
        self.clientid = clientid

    def __getattr__(self, name: str) -> Union[SPUser, Bucket]:
        return Api.__getitem__(self, name)

    def __getitem__(self, name: str) -> Union[SPUser, Bucket]:
        if name.lower() == "spuser":
            return SPUser(
                self.appname,
                self.token,
                host=self.host,
                scheme=self.scheme,
                clientid=self.clientid,
            )
        return Bucket(
            self.appname,
            self.token,
            name,
            userid=self.userid,
            host=self.host,
            scheme=self.scheme,
            clientid=self.clientid,
        )


class Admin(Api):
    def __init__(
        self,
        appname: str,
        admin_token: str,
        host: Optional[str] = None,
        scheme: str = "https",
        clientid: Optional[str] = None,
    ) -> None:
        self.appname = appname
        self.token = admin_token
        self.host = host
        self.scheme = scheme
        self.clientid = clientid

    def as_user(self, userid: str) -> Api:
        return Api(
            self.appname,
            self.token,
            userid=userid,
            host=self.host,
            scheme=self.scheme,
            clientid=self.clientid,
        )
