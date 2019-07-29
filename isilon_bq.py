from __future__ import print_function
import argparse
from concurrent import futures
from enum import Enum
from datetime import datetime
from google.cloud import bigquery, pubsub_v1
import isi_sdk_8_0 as isilon
import json
from isi_sdk_8_0.rest import ApiException
import logging
import os
import signal
from sys import argv
import threading
import urllib3

def patch_urllib(concurrency):
  class CustomHTTPSConnectionPool(urllib3.connectionpool.HTTPSConnectionPool):
    def __init__(self, *args,**kwargs):
      kwargs.update(maxsize=concurrency)
      super(CustomHTTPSConnectionPool, self).__init__(*args,**kwargs)

  urllib3.poolmanager.pool_classes_by_scheme['https'] = CustomHTTPSConnectionPool
  urllib3.disable_warnings() # BAD

def parseArguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', action='store', required=True, help="Isilon host address, e.g. https://example.org")
    parser.add_argument('--username', action='store', required=True, help="Unix username for Isilon API, e.g. 'CHARLES\\user'")
    parser.add_argument('--password', action='store', required=True, help="Unix password for Isilon API")
    parser.add_argument('--project', action='store', required=True, help="Google project ID")
    parser.add_argument('--topic', action='store', required=True, help="PubSub topic name")
    parser.add_argument('--subscription', action='store', required=True, help="PubSub subscription name")
    parser.add_argument('--dataset', action='store', required=True, help="BigQuery dataset name")
    parser.add_argument('--table', action='store', required=True, help="BigQuery table name")
    parser.add_argument('--concurrency', action='store', type=int, default=100, help="Maximum number of directories listed at a time")

    if len(argv[1:])==0:
        parser.print_help()
        parser.exit()

    return parser.parse_args()

def get_isilon_client(host, username, password):
  configuration = isilon.Configuration()
  configuration.host = host
  configuration.username = username
  configuration.password = password
  configuration.verify_ssl = False # BAD
  return isilon.NamespaceApi(isilon.ApiClient(configuration))

def get_publish(project, topic):
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project, topic)

  def publish(path, resume = None):
    data = {
      'path': path,
      'resume': resume,
    }
    message = json.dumps(data).encode('utf-8')
    return publisher.publish(topic_path, data=message)
  return publish

def get_path(dirent):
  return os.path.join(dirent.container_path, dirent.name)

def get_datetime(timestamp):
  return datetime.fromtimestamp(timestamp).isoformat()

class DirentType(Enum):
  container = 'dir'
  object = 'file'
  symbolic_link = 'symlink'

def get_type(type):
  if type in DirentType.__members__:
    return DirentType[type].value
  return type

def get_schema(field, type):
  return  bigquery.SchemaField(field, type, mode='REQUIRED')

def get_insert_rows(dataset, table):
  client = bigquery.Client()
  table_ref = client.dataset(dataset).table(table)
  schema = [
    get_schema('path', 'STRING'),
    get_schema('type', 'STRING'),
    get_schema('mode', 'STRING'),
    get_schema('size', 'INT64'),
    get_schema('owner', 'STRING'),
    get_schema('group', 'STRING'),
    get_schema('uid', 'INT64'),
    get_schema('gid', 'INT64'),
    get_schema('last_modified', 'TIMESTAMP'),
    get_schema('access_time', 'TIMESTAMP'),
    get_schema('create_time', 'TIMESTAMP'),
    get_schema('change_time', 'TIMESTAMP'),
    get_schema('is_hidden', 'BOOLEAN'),
    get_schema('nlink', 'INT64'),
    get_schema('inode', 'INT64'),
  ]
  table = bigquery.table.Table(table_ref, schema=schema)

  client.create_table(table, exists_ok=True)

  def insert_rows(id, dirents):
    if not dirents:
      return
    rows = []
    ids = []
    for i, row in enumerate(dirents):
      ids.append("{}.{}".format(id, i))
      rows.append({
        'path': get_path(row),
        'type': get_type(row.type),
        'mode': row.mode,
        'size': row.size,
        'owner': row.owner,
        'group': row.group,
        'uid': row.uid,
        'gid': row.gid,
        'last_modified': get_datetime(row.mtime_val),
        'access_time': get_datetime(row.atime_val),
        'create_time': get_datetime(row.btime_val),
        'change_time': get_datetime(row.ctime_val),
        'is_hidden': row.is_hidden,
        'nlink': row.nlink,
        'inode': row.id,
      })
    errors = client.insert_rows_json(table, rows, ids)
    if errors:
      print(errors)
  return insert_rows

DIRENTS_LIMIT = 10000
DIRENTS_DETAIL = ','.join([
  # 'container', ## skip
  'container_path',
  'name',

  'type',
  'mode',
  'is_hidden',
  'size',

  'gid',
  'group',
  'uid',
  'owner',

  # 'access_time', ## skip
  # 'change_time', ## skip
  # 'create_time', ## skip
  # 'last_modified', ## skip
  'atime_val',
  'btime_val',
  'ctime_val',
  'mtime_val',

  'id',
  'nlink',
  # 'block_size', ## skip
  # 'blocks', ## skip
  # 'stub', ## skip
])

def list_dir(client, path, resume = None):
  kwargs = {
    'detail': DIRENTS_DETAIL,
    'limit': DIRENTS_LIMIT,
  }

  if resume is not None:
    kwargs['resume'] = resume

  return client.get_directory_contents(path, **kwargs)

def listen_to_requests(project, topic, subscription, concurrency, dataset, table, isilon_client):
  logging.basicConfig()

  publish = get_publish(project, topic)
  insert_rows = get_insert_rows(dataset, table)

  subscriber = pubsub_v1.SubscriberClient()
  subscription_path = subscriber.subscription_path(project, subscription)
  flow_control = pubsub_v1.types.FlowControl(max_messages=concurrency)
  executor = futures.ThreadPoolExecutor(max_workers=concurrency)
  scheduler = pubsub_v1.subscriber.scheduler.ThreadScheduler(executor)
  callback = pubsub_callback(publish, insert_rows, isilon_client)
  subscriber.subscribe(subscription_path,callback, flow_control, scheduler)
  signal.pause()

def pubsub_callback(publish, insert_rows, isilon_client):
    def callback(message):
      data = json.loads(message.data)
      response = list_dir(isilon_client, data.get('path'), data.get('resume'))
      published = set()
      for dirent in response.children:
        if dirent.type == 'container':
          path = get_path(dirent).strip('/')
          published.add(publish(path))
      if response.resume is not None:
        published.add(publish(data['path'], response.resume))
      for p in published:
        p.result()
      insert_rows(message.message_id, response.children)
      message.ack()
    return callback

def main():
    args = parseArguments()
    patch_urllib(args.concurrency)
    isilon = get_isilon_client(
      args.host, args.username, args.password,
    )
    listen_to_requests(
      args.project, args.topic, args.subscription,
      args.concurrency, args.dataset, args.table,
      isilon,
    )

if __name__ == "__main__":
    main()
