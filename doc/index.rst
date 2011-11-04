.. WebStore Client documentation master file, created by
   sphinx-quickstart on Fri Jul  8 20:35:00 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. highlight:: python

WebStore Client: tables on the web
==================================

**WebStore Client** is a simple Python wrapper to easily access WebStore, 
a web-based table store used for on-line data storage, processing and 
visualization. WebStore supports various ways to access the data stored in
it, but this Python client library makes using it as simple as a generic 
`csv.DictWriter`.

Example
=======

To use WebStore, you need to have an instance of the WebStore server running
either locally or on the web. If you also want to have write access, you'll 
need valid access credentials. As WebStore doesn't handle authentication 
internally, this usually either means signing up with an associated instance
of `CKAN`_ or adding a user to an Apache `.htaccess` file.

.. _CKAN: http://ckan.org

Once you have both a server and credentials, you can start using WebClient by
creating a :class:`~webstore.client.Database` object::
  
  >>> from webstore.client import Database
  >>> database = Database('webstore.myserver.org', 'owner', 'mydatabase')

Note that each database has a user that owns it and that needs to be 
specified when connecting to a database. If you were to sign into your own 
database, the could would look like this::
  
  >>> database = Database('webstore.myserver.org', 'me', 'testdb',
                          http_user='me', http_password='secret')

Or, if you are using API key rather than user and password::

  >>> database = Database('webstore.myserver.org', 'me', 'testdb',
                          http_apikey='my-api-key')

There is no special command to create a database, so just connecting to an 
arbitrary name within your own namespace will create one. Once you have 
connected to a database, you can list tables or check for a specific name::

  >>> database.tables()
  [u'testdb', u'postal_codes', u'movies']
  >>> 'triples' in database
  False

To actually begin using a table, you can select a table::

  >>> table = database['weather']

... but what good is an empty table? So let's fill this thing with some 
rows::

  >>> table.writerow({'place': 'Berlin', 'temp': 23})
  >>> table.writerows([{'place': 'London', 'temp': 5},
                       {'place': 'Moscow', 'temp': -2}])

As you run this, both the table and the required columns are created
automatically. This means you don't need to worry about schema creation at 
all. You cannot, however, store complex objects like `dict`, `list`, `tuple`
or custom classes to WebStore.

While its simple to add new data, for updating existing rows, we use a little
trick: `unique_columns`. This set of column names will be used to try and 
perform an update::

  >>> table.writerow({'place': 'Berlin', 'temp': 18}, 
                     unique_columns=['place'])

This will update the `temp` values of all rows mentioning Berlin, but leave
any other columns intact.

Now that we have added some data to the table, we can try and traverse it::

  >>> for row in table:
  >>>     print row['place']
  Berlin
  London
  Moscow

Using :meth:`~webstore.client.Table.traverse` instead will give you the 
option to apply limits, offsets and very simple column filters::

  >>> for row in table.traverse(place='Berlin', _limit=4, _offset=0):
  >>>     print row['temp']
  18

For more informations on how you can use the WebStore client, have a look 
at the API documentation for :class:`~webstore.client.Table`.

API
===

Access to the WebStore client happens via two simple classes: 
:class:`~webstore.client.Database` and :class:`~webstore.client.Table`.

.. autofunction:: webstore.client.DSN

.. autofunction:: webstore.client.URL

.. autoclass:: webstore.client.Database
  :members: tables, __getitem__, __contains__

.. autoclass:: webstore.client.Table
  :members:
