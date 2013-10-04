#!/usr/bin/env python

import requests
import sys
import os
import ijson

from threading import Thread
from Queue import Queue

BASE_URL = 'http://api.crunchbase.com/v/1/%s.js'

TYPES = {
    'company': {
        'list_resource': 'companies'
    },
    'financial-organization': {
        'list_resource': 'financial-organizations'
    },
    'person': {
        'list_resource': 'people'
    },
    'product': {
        'list_resource': 'products'
    },
    'service-provider': {
        'list_resource': 'service-providers'
    }
}

N_WORKERS = 10

def get_api_key():
    with open('apikey.txt', 'r') as f:
        return f.read().strip()

API_KEY = get_api_key()

def get_url(resource):
    return BASE_URL % resource


def save(resource, filename, **params):
    params['api_key'] = API_KEY

    r = requests.get(get_url(resource), params=params, stream=True)

    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()

    return filename


def pull_type(typename, dest_dir):
    type_config = TYPES[typename]
    list_resource = type_config['list_resource']
    list_filename = '%s/%s.list.json' % (dest_dir, typename)
    document_dir = '%s/%s' % (dest_dir, typename)

    if not os.path.exists(document_dir):
        os.makedirs(document_dir)

    if not os.path.exists(list_filename):
        print 'Pulling %s list from "%s"...' % (typename, get_url(list_resource))
        save(list_resource, list_filename)


    q = Queue(N_WORKERS * 2)

    def download_worker():
        while True:
            entity_name = q.get()
            try:
                save('%s/%s' % (typename, entity_name), get_entity_filename(entity_name))
            except Exception, e:
                print 'Error: %s' % str(e)
            q.task_done()


    def get_entity_filename(entity_name):
        return '%s/%s.json' % (document_dir, entity_name[:25])


    for i in xrange(N_WORKERS):
        t = Thread(target=download_worker)
        t.daemon = True
        t.start()

    try:
        with open(list_filename, 'r') as f:
            for i, item in enumerate(ijson.items(f, 'item')):
                entity_name = item['permalink']
                this_filename = get_entity_filename(entity_name)

                if os.path.exists(this_filename):
                    print 'Already have %s %i ("%s")' % (typename, i, entity_name)
                    continue

                print 'Fetching %s %i ("%s")...' % (typename, i, entity_name)
                q.put(entity_name)
                
            q.join()
    except KeyboardInterrupt:
        sys.exit(1)


def pull_crunchbase(dest_dir, type_set):
    for typename in type_set:
        if typename not in TYPES:
            print 'Unknown type: %s' % typename

    for typename in TYPES:
        if typename not in type_set: continue
        print 'Pulling %s objects...' % typename
        pull_type(typename, dest_dir)


if __name__ == '__main__':
    try:
        dest_dir = sys.argv[1]
        type_set = set(sys.argv[2].split(',')) if len(sys.argv) > 2 else set(TYPES)
    except:
        print 'Usage: %s <output_dir> <optional,types,separated,by,comma>' % __file__
        print ''
        print 'Available types:\n  * %s' % '\n  * '.join(sorted(TYPES.keys()))
        sys.exit(1)

    pull_crunchbase(dest_dir, type_set)
