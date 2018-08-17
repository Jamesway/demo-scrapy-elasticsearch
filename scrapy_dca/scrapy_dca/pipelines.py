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
        self.params = { 'key': get_project_settings().get('MAPQUEST_KEY'), 'maxResults': 1, 'outFormat': 'json'}


    # turn a list of addresses into geo points
    def batch_process(self, addresses):

        for a in addresses:
            print('|' + a + '|')

        if not addresses or len(addresses) > 100:
            raise Exception('Invalid address list')

        self.params['location'] = addresses
        r = requests.get(self.url, self.params)

        # es takes a geopoint list as long first then lat
        geopoints = {}
        for result in r.json()['results']:
            print('******************')
            print('|' + result['providedLocation']['location'] + '|')
            if result['locations']:

                # since all we are taking in is a list of addresses, I use an md5 to identify the address coordinates in the result dictionary
                address_hash = md5(result['providedLocation']['location'].encode('utf8')).hexdigest()
                print(address_hash, "long/lat:", result['locations'][0]['latLng']['lng'], result['locations'][0]['latLng']['lat'])
                geopoints[address_hash] = [result['locations'][0]['latLng']['lng'], result['locations'][0]['latLng']['lat']]
            else:
                print('no geo data')
                geopoints[address_hash] = []
            print('******************')

        return geopoints


import certifi
from scrapy.utils.project import get_project_settings
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime

class ElasticSearchBulkPipeline(object):

    mapping = '''
        {
            "mappings": {
                "physician": {
                    "properties": {
                        "address": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "expiration": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "gender": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "language": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "license": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "license_type": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "location": {
                            "type": "geo_point"
                        },
                        "med_school": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "scraped_at": {
                            "type": "date"
                        },
                        "services": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                }
            }
        }
    '''

    def __init__(self):

        self.batchsize = 50
        self.items = []
        self.addresses = []

        self.geocoder = MapQuestGeocoder()

        # https://github.com/elastic/elasticsearch-py/issues/669#issuecomment-344348583
        self.es = Elasticsearch(
            get_project_settings().get('ELASTIC_SEARCH'),
            use_ssl=True,
            ca_certs=certifi.where()
        )

        if not self.es.indices.exists('provider'):

            self.es.indices.create('provider', body=self.mapping, ignore=400)


    def process_item(self, item, spider):

        # # hack for stopping spider https://stackoverflow.com/a/9699317
        # if hasattr('spider', 'date_limit') and item['published_at'] < spider.date_limit:
        #     # this switch hack is used in the spider
        #     spider.stop_spider = True
        #     return item

        # store items and process once we hit the batch size
        self.items.append(item)

        # store the addresses so we can batch forward geocode them
        self.addresses.append(item['address'])

        if len(self.items) >= self.batchsize:
            self.insert_items()

        return item


    def gen_data(self):

        # get the geopoints
        # es takes a geopoint list long first
        lnglat = self.geocoder.batch_process(self.addresses)

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
                    'location': lnglat[md5(item['address'].encode('utf8')).hexdigest()],
                    'name': item['name'],
                    'language': item['language'],
                    'gender': item['gender'],
                    'services': item['services'],
                    'med_school': item['school'],
                    'scraped_at': datetime.now(),
                },
            }

    def insert_items(self):

        bulk(self.es, self.gen_data())

        # reset
        self.addresses = []
        self.items = []


    def close_spider(self, spider):

        if self.items:
            self.insert_items()

        self.es.indices.refresh(index="provider")

        res = self.es.search(index="provider", body={"query": {"match_all": {}}})
        print("Got %d Hits:" % res['hits']['total'])
        print('****************************************')
        print('************closing spider**************')
        print('****************************************')