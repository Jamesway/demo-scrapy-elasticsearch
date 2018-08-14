# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


import requests
from hashlib import md5
class MapQuestGeocoder:

    def __init__(self):
        self.url = 'http://www.mapquestapi.com/geocoding/v1/batch'
        self.params = { 'key': 'bk1lWhV2ivKVPdplr4He0H1C31twOfAN', 'maxResults': 1, 'outFormat': 'json'}
        self.latlng = {}

    def batch_process(self, addresses):

        print('******************')
        print(str(len(addresses)))
        print('******************')
        for a in addresses:
            print('|' + a + '|')

        if not addresses or len(addresses) > 100:
            raise Exception('Invalid address list')

        self.params['location'] = addresses
        r = requests.get(self.url, (self).params)

        # print('******************')
        # print(r.url)
        # print('******************')


        for result in r.json()['results']:
            print('******************')
            print('|' + result['providedLocation']['location'] + '|')
            if result['locations']:
                address_hash = md5(result['providedLocation']['location'].encode('utf8')).hexdigest()
                print(address_hash, result['locations'][0]['latLng']['lat'], result['locations'][0]['latLng']['lng'])
                self.latlng[address_hash] = [result['locations'][0]['latLng']['lat'], result['locations'][0]['latLng']['lng']]
            else:
                print('no geo data')
                self.latlng[address_hash] = []
            print('******************')
            #

        return self.latlng


import uuid, certifi
# from sqlalchemy.orm import sessionmaker
# from .models import ArticleDB, db_connect, create_table
from scrapy.utils.project import get_project_settings
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
import geocoder
from time import sleep

class ElasticSearchBulkPipeline(object):

    def __init__(self):

        self.batchsize = 5
        self.items = []
        self.addresses = []
        self.latlng = {}

        # https://github.com/elastic/elasticsearch-py/issues/669#issuecomment-344348583
        self.es = Elasticsearch(
            get_project_settings().get('ELASTIC_SEARCH'),
            # http_auth=(get_project_settings().get('ELASTICSEARCH_USER'), get_project_settings().get('ELASTICSEARCH_PASSWORD')),
            use_ssl=True,
            ca_certs=certifi.where()
        )

    def process_item(self, item, spider):

        # # hack for stopping spider https://stackoverflow.com/a/9699317
        # if hasattr('spider', 'date_limit') and item['published_at'] < spider.date_limit:
        #     # this switch hack is used in the spider
        #     spider.stop_spider = True
        #     return item

        # store items and process once we hit the batch size
        self.items.append(item)

        # store the addresses so we can bulk forward geocode them
        self.addresses.append(item['address'])

        if len(self.items) >= self.batchsize:
            self.insert_items()

        return item

    def gen_geocode(self):


        g = MapQuestGeocoder()
        self.latlng = g.batch_process(self.addresses)

        sleep(2)



    def gen_data(self):


        for item in self.items:
            yield {
                '_index': 'provider',
                '_type': 'physician',
                '_id': item['license'],
                '_source': {
                    'license': item['license'],
                    'license_type': item['license_type'],
                    'expiration': item['exp_date'],
                    'address': item['address'],
                    'location': self.latlng[md5(item['address'].encode('utf8')).hexdigest()],
                    'name': item['name'],
                    'language': item['language'],
                    'gender': item['gender'],
                    'services': item['services'],
                    'scraped_at': datetime.now(),
                },
            }

    def insert_items(self):
        # generate the forward geocode data
        self.gen_geocode()

        bulk(self.es, self.gen_data())

        # reset
        self.addresses = []
        self.latlng = {}
        self.items = []


    def close_spider(self, spider):

        if self.items:
            self.insert_items()

        self.es.indices.refresh(index="provider")

        res = self.es.search(index="provider", body={"query": {"match_all": {}}})
        print("Got %d Hits:" % res['hits']['total'])
        # for hit in res['hits']['hits']:
        #     print("%(title)s %(published_at)s" % hit["_source"])

        print('****************************************')
        print('************closing spider**************')
        print('****************************************')




class ElasticSearchPipeline(object):

    def __init__(self):

        self.batchsize = 20
        self.items = []

        # https://github.com/elastic/elasticsearch-py/issues/669#issuecomment-344348583
        self.es = Elasticsearch(
            'https://a112d83881ce4894a1a049c362b620a2.us-east-1.aws.found.io:9243',
            http_auth=('elastic', 'uUD7ENQLvGnERXns84IY3bQe'),
            use_ssl=True,
            ca_certs=certifi.where()
        )


    def process_item(self, item, spider):


        # hack for stopping spider https://stackoverflow.com/a/9699317
        if hasattr('spider', 'date_limit') and item['published_at'] < spider.date_limit:
            # this switch hack is used in the spider
            spider.stop_spider = True
            return item

        doc = {
            'doi': item['doi'],
            'journal': item['journal'],
            'authors': item['authors'],
            'title': item['title'],
            'published_at': item['published_at'],
            'scraped_at': datetime.now(),
        }
        # create the index
        res = self.es.index(index="content", doc_type='article', id=item['doi'], body=doc)
        print(res['result'])
        self.es.indices.refresh(index="content")
        #
        # res = es.get(index="test-index", doc_type='article', id=1)
        # print(res['_source'])
        #
        # es.indices.refresh(index="test-index")
        #
        # res = es.search(index="test-index", body={"query": {"match_all": {}}})
        # print("Got %d Hits:" % res['hits']['total'])
        # for hit in res['hits']['hits']:
        #     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

        return item




from sqlalchemy.orm import sessionmaker
from .models import AppointmentDB, PracticeDB, db_connect, create_table
import uuid, random, json
from .appointment_calc import  apppointment_calc

class ScrapyDcaPipeline(object):

    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)


    def process_item(self, item, spider):

        """
        This method is called for every item pipeline component.
        """
        #
        # # hack for stopping spider https://stackoverflow.com/a/9699317
        # if (hasattr('spider', 'date_limit') and item['published_at'] < spider.date_limit):
        #     # this switch hack is used in the spider
        #     spider.stop_spider = True
        #     return item

        session = self.Session()

        practice_id = uuid.uuid4()


        db1 = PracticeDB()
        db1.id = practice_id
        db1.license = item['license']
        db1.license_type = item['license_type']


        if (random.randint(0,1) is 1):
            db1.physician_name = 'Dr. ' + item['name'].title()
        else:
            db1.physician_name = item['name'].title() + ' MD'


        db1.address = json.dumps(item['address'])
        #db1.practice_location = json.dumps(item['practice_location'])
        #db1.services = json.dumps(item['services'])

        prices = [70, 75, 79, 80, 85, 89, 90, 95, 99, 100, 105]
        db1.base_price = prices[random.randint(0, len(prices) -1)]
        db1.scraped_at = item['scraped_at']


        appts = []
        for t in apppointment_calc(random.randrange(2, 5)):

            db2 = AppointmentDB()
            db2.id = uuid.uuid4()
            db2.practice_id = practice_id
            db2.appointment_time = t
            appts.append(db2)

        # specialties = []
        # for s in item['services']:
        #
        #     db3 = SpecialtiesDB()
        #     db3.practice_id = practice_id
        #     db3.specialty = s
        #     specialties.append(db3)

        try:
            session.add(db1)
            for a in appts:
                session.add(a)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
