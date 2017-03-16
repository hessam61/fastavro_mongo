from contextlib import contextmanager
from datetime import (
    datetime,
    timedelta)
from os import (
    path,
    makedirs)
from timeit import Timer
from time import clock, mktime

from hdfs import InsecureClient
from hdfs.ext.avro import AvroReader, AvroWriter
from pymongo import MongoClient
from yaml import safe_load as yaml_load

with open('mongo_creds.yml') as credsfile:
    creds = yaml_load(credsfile)

hdfs_client = InsecureClient('http://localhost:14000', user='cloudera')
mongo_client = MongoClient(host='uphsvlndc058.uphs.upenn.edu',port=27017)
is_authed = mongo_client.admin.authenticate(creds['user'],creds['pass'])

psPreds = mongo_client.psPreds.preds


def generate_avro_schema():
    q = {'modelName':'sepsismodel_noEpoch'}
    predictions = list(psPreds.find(q).limit(1))
    # FIXME: use extend instead
    fields = [{'name': key, 'type': ['float', 'null']}
        for key in predictions[0]['features'].keys()]
    fields.append({
        'name': 'event_id',
        'type': 'string'})
    fields.append({
        'name': 'valid_on',
        'type': ['int', 'null'],
        'logicalType': 'time-millis'})
    fields.append({
        'name': 'created_on',
        'type': ['int', 'null'],
        'logicalType': 'time-millis'})
    fields.append({
        'name': 'input_events',
        'type': 'string',
        'doc': 'Array of the penn signal event_ids that we\'re used as input into the transform output'})
    fields.append({'name': 'patient_id', 'type': ['int', 'null']})
    fields.append({
        'name': 'provenance',
        'type':{
            'type': 'array',
            'items': {
                'name': 'source',
                'type': 'string'
            }}
        })
    return {
        'type': 'record',
        'name': 'Transform',
        'doc': 'The transform is either scalar or vector (array). The ' +
            'indices are held in key value pairs in the schema.  The types ' +
            'of the values are unique to the transform component.  ' +
            'Typically the values will be floats.',
        'fields': fields
    }

def get_mongo_predictions(starttime, endtime, limit=None):
    q = {'modelName':'sepsismodel',
         'WCT':{'$gte':starttime,'$lt':endtime}}
    r = psPreds.find(q,{'WCT':1,'VISIT_NUMBER':1,'features':1,
        '_id':1})
    return r if limit is None else r.limit(limit)


def write_avro(file_name, predictions):
    avro_schema = generate_avro_schema()
    print('predictions type: ', type(predictions))
    print('predictions length: ', len(predictions))
    with AvroWriter(hdfs_client, file_name, schema=avro_schema, overwrite=True) as writer:
        for prediction in predictions:
            print(len(prediction))
            print(type(prediction['features']))
            data = {key: float(value) for key, value in prediction['features'].items()}
            data['event_id'] = str(prediction['_id'])
            data['valid_on'] = int(prediction['WCT'].timestamp())
            data['created_on'] = int(prediction['WCT'].timestamp())
            data['input_events'] = str(prediction['_id'])
            data['patient_id'] = prediction['VISIT_NUMBER']
            data['provenance'] = ['psPredsExtract', 'TransformSepsis']
            writer.write(data)

@contextmanager
def timer(name):
    start = clock()
    try:
        yield
    finally:
        print('{} {}'.format(name, clock() - start))


if __name__=='__main__':
    with timer('Schema:'):
        generate_avro_schema()

    endtime = datetime.now()
    endtime = datetime(endtime.year,endtime.month,endtime.day)

    for i in range(1):
        starttime = endtime - timedelta(1)
        file_name = ''.join(('transforms/',
            starttime.strftime("%Y"), '/',
            starttime.strftime("%m"), '/',
            starttime.strftime("%Y_%m_%d"), '_transform.avro'))
        with timer('Mongo: '):
            predictions = list(get_mongo_predictions(starttime, endtime, 1))
        with timer('Avro:  '):
            write_avro(file_name, predictions)
        endtime=starttime
