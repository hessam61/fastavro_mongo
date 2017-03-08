from contextlib import contextmanager
from datetime import (
    datetime,
    timedelta)
from os import (
    path,
    makedirs)
from timeit import Timer
from time import clock, mktime
from sys import getsizeof

from fastavro import writer
from pymongo import MongoClient
from yaml import safe_load as yaml_load

with open('mongo_creds.yml') as credsfile:
    creds = yaml_load(credsfile)

client = MongoClient(host='uphsvlndc058.uphs.upenn.edu',port=27017)
is_authed = client.admin.authenticate(creds['user'],creds['pass'])

psPreds = client.psPreds.preds


def generate_avro_schema():
    q = {'modelName':'sepsismodel_noEpoch'}
    predictions = list(psPreds.find(q).limit(1))

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
        'type': 'null',
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
         'WCT':{'$gt':starttime,'$lt':endtime}}
    r = psPreds.find(q,{'WCT':1,'VISIT_NUMBER':1,'features':1,
        '_id':1})
    return r if limit is None else r.limit(limit)

def get_features(file_name, predictions):
    schema = generate_avro_schema()
    with open(file_name, 'wb') as out:
        writer(out, schema, to_avro(predictions))

def to_avro(predictions):
    for prediction in predictions:
        time = to_timestamp(prediction['WCT'])
        data = {key: float(value) for key, value in prediction['features'].items()}
        data['event_id'] = str(prediction['_id'])
        data['valid_on'] = time
        data['created_on'] = time
        data['input_events'] = ''
        data['patient_id'] = prediction['VISIT_NUMBER']
        data['provenance'] = ['psPredsExtract', 'TransformSepsis']
        yield data

def to_timestamp(date):
    raw_date = str(date).split(".")[0]
    time_format = "%Y-%m-%d %H:%M:%S"
    return int(mktime(datetime.strptime(raw_date, time_format).timetuple()))

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

    for i in range(365):
        starttime = endtime - timedelta(10)
        #feats = get_features(starttime,endtime)
        file_name = ''.join(('',
            starttime.strftime("%Y_%m_%d"), '-',
            endtime.strftime("%Y_%m_%d"), '_transform.avro'))
        with timer('Mongo: '):
            predictions = list(get_mongo_predictions(starttime, endtime, 1000))
        with timer('Avro:  '):
            get_features(file_name, predictions)
        endtime=starttime
