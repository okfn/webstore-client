import os
from urlparse import urljoin, urlparse
from urllib import urlencode
try:
    from json import loads, dumps
except ImportError:
    from simplejson import loads, dumps

import ConfigParser
from httplib import HTTPConnection

ASCENDING = 'asc'
DESCENDING = 'desc'

def DSN(name, config_file=None):
    """ Create a database from a data source name.

    Allows to connect to pre-configured databases via a config file, 
    either in the current working directory (``webstore.cfg``) or the 
    user's home directory (``.webstore.cfg``). The configuration is 
    expected to have the following format::

      [DEFAULT]
      # global options
      server = webstore.server.org
      http_user = username
      http_password = password

      [source1]
      user = username
      database = db1

    If the given ``name`` does not exist as a section in the 
    configuration file, the ``DEFAULT`` section will be used and the 
    name will be assumed to be the target database name.
    """
    config = ConfigParser.SafeConfigParser()
    if config_file:
        config.read([config_file])
    else:
        config.read(['webstore.cfg', 
                    os.path.expanduser('~/.webstore.cfg')])
    sect = name
    if not config.has_section(name):
        sect = 'DEFAULT'
    message = 'DataSource config: No "%s" given, please set up the ' \
        + 'config file (.webstore.cfg). See http://bit.ly/webstore-dsn.'
    for opt in ['server', 'user']:
        if not config.has_option(sect, opt):
            raise ValueError(message % opt)
    database = config.get(sect, 'database') if \
            config.has_option(sect, 'database') else name
    http_user = config.get(sect, 'http_user') if \
            config.has_option(sect, 'http_user') else name
    http_password = config.get(sect, 'http_password') if \
            config.has_option(sect, 'http_password') else name
    return Database(config.get(sect, 'server'),
                    config.get(sect, 'user'),
                    database, http_user=http_user,
                    http_password=http_password)


def URL(url, default_table=None):
    """ Create a webstore database handle from a URL.
    The URL is assumed to have the following form::

        http://user:password@server/db_user/db_database[/table]

    If no ``user`` and ``password`` are given, anonymous access is 
    used. The additional ``table`` argument is optional: if it is 
    present, a tuple of (``Database``, ``Table``) objects will be 
    returned. If no ``table`` is specfied, the second element of the
    tuple will be ``None`` or the table named after the optional 
    argument ``default_table``."""
    parsed = urlparse(url)
    path = parsed.path.split('/')[1:]
    if len(path) < 2:
        raise ValueError("Incomplete webstore DB path: %s" % parsed.path)
    db = Database(parsed.hostname, path[0], path[1], port=parsed.port,
                  http_user=parsed.username, http_password=parsed.password)
    table = None
    if len(path) > 2:
        table = db[path[2]]
    elif default_table is not None:
        table = db[default_table]
    return (db, table)


class WebstoreClientException(Exception):
    """ A simple exception for webstore errors which have been
    transmitted to the client. Note that some success messages are 
    also first encoded as errors with a `state` of 'success'.
    """

    def __init__(self, response, body):
        self.body = body
        self.response = response

    @property
    def message(self):
        return self.body.get('message')

    @property
    def state(self):
        return self.body.get('state')

    @property
    def url(self):
        return self.body.get('url')

    def __unicode__(self):
        return self.message

    def __str__(self):
        return self.message.encode('utf-8')

    def __repr__(self):
        return "<WebstoreClientException(%s: %s)>" (self.state, 
                                                    self.message)

class _Base(object):
    """ Common base object for ``Database`` and ``Table``. Does basic
    HTTP connectivity and decoding/encoding. """

    def __init__(self, server, port, base_path, http_user, http_password):
        self.server = server
        self.port = port or 80
        self.base_path = base_path
        self.authorization = None
        if http_user is not None and http_password is not None:
            secret = http_user + ':' + http_password
            self.authorization = 'Basic ' + secret.encode('base64')

    def _raw_request(self, method, path, data=None, headers={}):
        """ Run a raw request, handle authentication but no
        decoding/encoding. """
        _headers = {}
        if self.authorization:
            _headers['Authorization'] = self.authorization
        _headers.update(headers)
        path = urljoin(self.base_path, path)
        _headers['Content-Length'] = len(data)+2 if data else 0
        conn = HTTPConnection(self.server, self.port)
        conn.request(method, path, data, _headers)
        response = conn.getresponse()
        return response

    def _request(self, method, path, data=None, headers={}):
        """ Run a request against the webstore, using JSON as a 
        default representation. """
        _headers = headers.copy()
        if not 'Content-Type' in _headers:
            _headers['Content-Type'] = 'application/json'
            if data is not None:
                data = dumps(data)
        if not 'Accept' in _headers:
            _headers['Accept'] = 'application/json'
        response = self._raw_request(method, path, data, _headers)
        try:
            data = response.read()
            data = loads(data)
        except ValueError:
            data = {'state': 'error', 'message': response.reason}
        if isinstance(data, dict) and 'state' in data and 'message' in data:
            raise WebstoreClientException(response, data)
        return data


class Database(_Base):
    """ A web-based database with many `Tables`. Databases are owned by 
    one particular user and can usually only be written by this user. """

    def __init__(self, server, database_user, database_name, 
            port=None, http_user=None, http_password=None):
        """ Create a new database connection to the server `server_url`.

        This will create an object that allows the creation and management
        of databases on webstore.

        If both `http_user` and `http_password` are given, authentication 
        via HTTP Basic is attempted. Note that invalid credentials will 
        lead to errors even for requests that normally do not require a 
        user to be signed in.

        :Parameters:
            - `server`: hostname or IP of the server to connect to. Note 
              that this is not a URL but only the host name.
            - `database_user`: name of the user owning the database.
            - `database_name`: the database name. Note that database names
              can only contain alphanumeric characters and underscores and
              must not start with a number or underscore.
            - `port`: server port, defaults to 80.
            - `http_user`: the username for HTTP authentication. For 
              webstores connected to an instance of CKAN, this is the user
              that signs into CKAN.
            - `http_password`: the user's password.

        """
        self.database_user = database_user
        self.database_name = database_name
        self.http_user = http_user
        self.http_password = http_password
        assert not '/' in server, "Server hostname most not contain '/'!"
        base_path = '/' + database_user + '/' + database_name
        super(Database, self).__init__(server, port, base_path,
                http_user, http_password)


    def __contains__(self, table_name):
        """ Check if `table_name` is an existing table on the database. 

        :Parameters:
            - `table_name`: the table name to check for.
        """
        return table_name in self.tables()

    def tables(self):
        """ Get a list of the tables defined in this database. """
        response = self._request("GET", '')
        return [r.get('name') for r in response]

    def __getitem__(self, table_name):
        """ Get a table from the database by name. 

        :Parameters:
            - `table_name`: name of the table to return.
        """
        return Table(self.server, self.port, self.base_path, table_name,
                     self.http_user, self.http_password)

    def __repr__(self):
        return "<Database(%s / %s)>" % (self.database_user,
                                        self.database_name)


class Table(_Base):
    """ A table in the database on which you can perform read 
    and (if authorized) write operations. """

    def __init__(self, server, port, base_path, table_name, http_user,
                 http_password):
        """ Get a handle for the table `table_name` on `server`.

        *Note*: This is usually created via database[table_name].

        This will create an object that allows the creation and 
        management of a table on webstore.

        If both `http_user` and `http_password` are given, authentication 
        via HTTP Basic is attempted. Note that invalid credentials will 
        lead to errors even for requests that normally do not require a 
        user to be signed in.

        :Parameters:
            - `server`: hostname or IP of the server to connect to. Note 
              that this is not a URL but only the host name.
            - `port`: server port, defaults to 80.
            - `base_path`: the path prefix on the server, e.g. 
              /<user>/<database>.
            - `table_name`: the name of the table. The table name must
              contain only alphanumeric characters and underscores and
              must not start with a number or underscore.
            - `http_user`: the username for HTTP authentication. For 
              webstores connected to an instance of CKAN, this is the user
              that signs into CKAN.
            - `http_password`: the user's password.
        """
        self.table_name = table_name
        self.unique_columns = []
        base_path = base_path + '/' + table_name
        self._exists = False
        super(Table, self).__init__(server, port, base_path,
                http_user, http_password)

    def exists(self):
        """ Check if the given table actually exists or if it needs to be
        created by writing content. 

        Note the value is cached and the method thus failes to recognize 
        write activity from other places.
        """
        if not self._exists:
            try:
                self._request('GET', '?_limit=0')
                self._exists = True
            except WebstoreClientException:
                self._exists = False
        return self._exists

    def traverse(self, _step=50, _sort=[], _limit=None, _offset=0, 
                 **kwargs):
        """ Iterate over the table, fetching `_step` items at a time.

        This will return a generator to traverse the table and yield each
        row as a dictionary of column values.

        :Parameters:
            - `_step`: determines how many records will be retrieved with
              each request. This is mostly a tuning aspect.
            - `_limit`: the maximum number of elements to retrieve.
            - `_offset`: offset to start traversal at.
            - `_sort`: a list of sorting parameters given as tuples of 
              (column, direction). The `direction` can either be 'asc' or
              'desc'.
            - other keyword arguments: will be passed to the server and 
              treated as column filters. 
        """
        if _limit is not None:
            _step = min(_step, _limit)
        query = kwargs.items()
        query.extend([('_sort', '%s:%s' % (v, k)) for k, v in _sort])
        while _limit is None or _offset < _limit:
            page_query = list(query)
            page_query.append(('_offset', _offset))
            page_query.append(('_limit', _offset + _step))
            qs = urlencode([(k, unicode(v).encode('utf-8')) for \
                            k, v in page_query])
            result = self._request("GET", '?' + qs)
            for row in result:
                yield row
            if len(result) < _step:
                break
            _offset += _step

    def find_one(self, **kwargs):
        """ Get a single item matching the given criteria. The criteria 
        can be the value of any column. If no item is found, ``None`` is
        returned. """
        try:
            items = list(self.traverse(_limit=1, **kwargs))
            if not len(items):
                return None
            return items[0]
        except WebstoreClientException:
            return None

    def writerow(self, row, unique_columns=None):
        """ Write a single row. The row is expected to be a flat
        dictionary (i.e. no lists, tuples or dicts as values).

        For more documentation, see `writerows`. 
        """
        return self.writerows([row], unique_columns=unique_columns)

    def writerows(self, rows, unique_columns=None):
        """ Write a set of rows to the table. Each row is expected to be
        a flat dictionary (i.e. no lists, tuples or dicts as values).

        When `unique_columns` is set, webstore will first attempt to 
        update existing rows that share the values of each column in the
        set. If no update can be performed, a new row will instead be 
        inserted.

        :Parameters:
            - `rows`: a list of rows to be written to the table.
            - `unique_columns`: a set of columns that can be used to 
              uniquely identify this row when attempting to update.
        """
        try:
            unique_columns = unique_columns or self.unique_columns
            query = '?' + urlencode([('unique', u) for u in unique_columns])
            if self.exists():
                res = self._request("PUT", query, rows)
            else:
                res = self._request("POST", '', rows)
            self._exists = True
            return res
        except WebstoreClientException, wce:
            if wce.state != 'success':
                raise

    def distinct(self, column_name):
        """ Get all distinct values for a column. """
        return self._request('GET', self.table_name + '/distinct/' + column_name)

    def __iter__(self):
        """ Defer iteration to traverse. """
        return iter(self.traverse())

    def delete(self):
        """ Delete the table entirely, dropping its structure as well 
        as any contained data.
        """
        try:
            return self._request("DELETE", '')
        except WebstoreClientException, wce:
            if wce.state != 'success':
                raise
            else:
                self._exists = False

    def __repr__(self):
        return "<Table(%s)>" % self.table_name

