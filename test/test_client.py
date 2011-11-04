import sys
import unittest
import tempfile

from webstore.client import Database, Table, WebstoreClientException

from threading import Thread
try:
    from webstore.web import app
except ImportError:
    print >>sys.stderr, "To run the tests, the server must be installed"
    sys.exit()

def run_webstore():
    app.config['SQLITE_DIR'] = tempfile.mkdtemp()
    app.config['TESTING'] = True
    app.config['AUTHORIZATION']['world'] = \
                ['read', 'write', 'delete']
    app.run(port=6675)
server = Thread(target=run_webstore, name='server')
server.daemon = True
server.start()
from time import sleep
sleep(1)

FIXTURES = [{'place': 'Berlin', 'temp': '5', 'humidity': '0.6'},
            {'place': 'Novosibirsk', 'temp': '-10', 'humidity': '0.8'},
            {'place': 'Kairo', 'temp': '35', 'humidity': '0.9'},
            {'place': 'London', 'temp': '1', 'humidity': '0.1'}
            ]

class WebstoreClientTestCase(unittest.TestCase):
    
    def setUp(self):
        self.server_url = 'localhost'
        self.port = 6675
        # self.port = 5001
        self.database = Database(self.server_url, 'test', 'test',
                port=self.port)
        self.table = self.database['test']
        self.table.writerows(FIXTURES)

    def tearDown(self):
        try:
            self.table.delete()
        except WebstoreClientException:
            pass

    def test_database_table_listing(self):
        assert 'test' in self.database.tables(), self.database.tables()
        assert 'foo' not in self.database.tables(), self.database.tables()

    def test_database_contains(self):
        assert 'test' in self.database, self.database.tables()
        assert 'foo' not in self.database, self.database.tables()
    
    def test_database_getitem(self):
        test = self.database['test']
        assert isinstance(test, Table), test

    def test_table_traverse_full(self):
        all = list(self.table)
        assert len(all)==len(FIXTURES), all

    def test_table_traverse_filter(self):
        bln = list(self.table.traverse(place='Berlin'))
        assert len(bln)==1, bln
        assert bln[0]['place']=='Berlin', bln

    def test_table_traverse_limit(self):
        two = list(self.table.traverse(_limit=2))
        assert len(two)==2, two

    def test_table_add_row(self):
        row = {'place': 'Tokyo', 'radiation': '5usv'}
        self.table.writerow(row)
        all = list(self.table)
        assert len(all)==len(FIXTURES)+1, all
        tok = list(self.table.traverse(place='Tokyo'))
        assert tok[0]['radiation']=='5usv', tok
    
    def test_table_update_row(self):
        row = {'place': 'Berlin', 'radiation': '5usv'}
        self.table.writerow(row, unique_columns=['place'])
        all = list(self.table)
        assert len(all)==len(FIXTURES), all
        tok = list(self.table.traverse(place='Berlin'))
        assert tok[0]['radiation']=='5usv', tok
    
    def test_table_delete(self):
        self.table.delete()
        assert not 'test' in self.database


if __name__ == '__main__':
    unittest.main()




