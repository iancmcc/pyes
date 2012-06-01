import json
import unittest
import logging
from pprint import pprint
from datetime import datetime, timedelta

from pyelasticsearch import ElasticSearch

from pyes.nlquery.query import *
from pyes.nlquery.parser import LuceneParser

from pyes.es import ES as ElasticSearch
from pyes.query import *
from pyes.filters import *


class ElasticSearchTestCase(unittest.TestCase):

    def search(self, query):
        return self.conn.search_raw(query, 'test-index')

    def setUp(self):
        self.conn = ElasticSearch('http://localhost:9200/')

    def tearDown(self):
        self.conn.delete_index("test-index")

    def assertResultContains(self, result, expected):
        for (key, value) in expected.items():
            self.assertEquals(value, result[key])



class TestSearch(ElasticSearchTestCase):

    def testSimpleTerm(self):
        self.conn.index({'name':'gillian'}, 'test-index', 'test-type')
        self.conn.index({'name':'Ian'}, 'test-index', 'test-type')
        self.conn.index({'name':'Ian McCracken'}, 'test-index', 'test-type')
        self.conn.refresh(['test-index'])

        query = compile_query('name:gillian')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query('gillian')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query('name:"Ian McCracken"')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)


    def testWildcard(self):
        self.conn.index({'name':'ianmccracken'}, 'test-index', 'test-type', 1)
        self.conn.index({'name':'ianmacraken'}, 'test-index', 'test-type', 2)
        self.conn.index({'name':'iancmccracken'}, 'test-index', 'test-type', 3)
        self.conn.refresh(['test-index'])

        query = compile_query("name:ian*")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 3)

        query = compile_query("name:ian*mccracken")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 2)

        query = compile_query("name:*mac*")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query("name:ian?m?crac*")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query("name:ian*m?crac*")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 2)

    def testBoost(self):
        self.conn.index({'name':'ianmccracken'}, 'test-index', 'test-type', 1)
        self.conn.index({'name':'ianmacraken'}, 'test-index', 'test-type', 2)
        self.conn.index({'name':'iancmccracken'}, 'test-index', 'test-type', 3)
        self.conn.refresh(['test-index'])

        query = compile_query("name:*mac*^2.0 name:ian*")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 3)
        # Boost doesn't work yet because it's filters not queries
        #self.assertEqual(result['hits']['hits'][0]['_id'], '2')


    def testNumRange(self):
        self.conn.index({'a': 1}, 'test-index', 'test-type', 1)
        self.conn.index({'a': 5}, 'test-index', 'test-type', 2)
        self.conn.index({'a': 10}, 'test-index', 'test-type', 3)
        self.conn.refresh(['test-index'])

        query = compile_query("a:[1 to 10]")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 3)

        query = compile_query("a:[1 to 10}")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 2)

        query = compile_query("a:{1 to 10}")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

    def testDateRange(self):
        now = datetime.now()
        today = datetime.date(now)
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        fivedaysago = today - timedelta(days=5)
        infivedays = today + timedelta(days=5)
        self.conn.index({'a': today}, 'test-index', 'test-type')
        self.conn.index({'a': yesterday}, 'test-index', 'test-type')
        self.conn.index({'a': tomorrow}, 'test-index', 'test-type')
        self.conn.index({'a': fivedaysago}, 'test-index', 'test-type')
        self.conn.index({'a': infivedays}, 'test-index', 'test-type')
        self.conn.refresh(['test-index'])

        query = compile_query("a:[midnight yesterday to day after tomorrow at 11:59pm]")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 3)
        
        query = compile_query("a:[midnight last monday to 11:59pm in 6 days]")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 5)

        query = compile_query("a:[midnight to 11:59pm]")
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

    def testSubquery(self):
        self.conn.index({'name':'abcdefg', 'title': '1234'}, 'test-index', 'test-type', 1)
        self.conn.index({'name':'defghij', 'title': '5678'}, 'test-index', 'test-type', 2)
        self.conn.index({'name':'hijklmn', 'title': '1234'}, 'test-index', 'test-type', 3)
        self.conn.refresh(['test-index'])

        query = compile_query('name:abcde* and (title:1234 or title:5678)')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query('(name:*a* or name:*h*) and (title:1234 or title:5678)')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 3)

    def testBoolean(self):
        self.conn.index({'name':'abcdefg', 'title': '1234'}, 'test-index', 'test-type', 1)
        self.conn.index({'name':'defghij', 'title': '5678'}, 'test-index', 'test-type', 2)
        self.conn.index({'name':'hijklmn', 'title': '1234'}, 'test-index', 'test-type', 3)
        self.conn.refresh(['test-index'])

        query = compile_query('name:*de* and title:1234')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query('name:*de* -title:1234')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query('name:*de* title:1234')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 3)

        query = compile_query('-name:*de* title:1234')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query('+name:*de* and -title:1234')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 1)

        query = compile_query('name:abcdefg and not title:1234')
        result = self.search(query)
        self.assertEqual(result['hits']['total'], 0)


if __name__ == "__main__":
    unittest.main()

