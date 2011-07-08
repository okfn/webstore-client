import sys
import unittest
import tempfile

from threading import Thread
try:
    from webstore.web import app
except ImportError:
    print >>sys.stderr, "To run the tests, the server must be installed"
    sys.exit()

def run_webstore():
    sys.stdout = open('/dev/null', 'w')
    sys.stderr = open('/dev/null', 'w')
    app.config['SQLITE_DIR'] = tempfile.mkdtemp()
    app.config['TESTING'] = True
    app.run(port=6675)
server = Thread(target=run_webstore, name='server')
server.daemon = True
server.start()
from time import sleep
sleep(1)

class WebstoreClientTestCase(unittest.TestCase):
    
    def setUp(self):
        self.server_url = 'http://localhost:6675'
    
    def tearDown(self):
        pass

    def test_contact_server(self):
        import urllib2
        fh = urllib2.urlopen(self.server_url)
        print fh.read()
        fh.close()


if __name__ == '__main__':
    unittest.main()




