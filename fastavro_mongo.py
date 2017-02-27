from contextlib import contextmanager
from datetime import (
    datetime,
    timedelta)
from os import (
    path,
    makedirs)
from timeit import Timer
from time import clock
from sys import getsizeof, stdout

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
        'name': 'WCT',
        'type': ['int', 'null'],
        'logicalType': 'time-millis'})
    fields.append({'name': 'VISIT_NUMBER', 'type': ['int', 'null']})
    return {
        'type': 'record',
        'name': 'Transform',
        'doc': 'The transform is either scalar or vector (array). The ' +
            'indices are held in key value pairs in the schema.  The types of the' +
            'values are unique to the transform component.  Typically the' +
            'values will be floats.',
        'fields': fields
    }

def get_mongo_predictions(starttime, endtime, limit=None):
    q = {'modelName':'sepsismodel',
         'WCT':{'$gt':starttime,'$lt':endtime}}
    r = psPreds.find(q,{'WCT':1,'VISIT_NUMBER':1,'features':1,
        '_id':0})
    return r if limit is None else r.limit(limit)

def get_features(directory, file_name, predictions):
    schema = generate_avro_schema()
    if not path.exists(directory):
        makedirs(directory)
    with open(file_name, 'wb') as out:
        writer(out, schema, to_avro(predictions))

def to_avro(predictions):
    for i in predictions:
        data = {key: float(value) for key, value in i['features'].items()}
        data['WCT'] = int(i['WCT'].timestamp())
        data['VISIT_NUMBER'] = i['VISIT_NUMBER']
        yield data


@contextmanager
def timer(name):
    start = clock()
    try:
        yield
    finally:
        print('{} {} log'.format(name, clock() - start))


if __name__=='__main__':

    with timer('Schema:'):
        generate_avro_schema()

    endtime = datetime.now()
    endtime = datetime(endtime.year,endtime.month,endtime.day)

    for i in range(1):
        starttime = endtime - timedelta(8)
        directory = ''.join((
            starttime.strftime("%Y_%m_%d"), '-',
            endtime.strftime("%Y_%m_%d")))
        file_name = ''.join((
            starttime.strftime("%Y_%m_%d"), '-',
            endtime.strftime("%Y_%m_%d"), '/features.avro'))
        print(starttime)
        print(endtime)
        with timer('Mongo: '):
            predictions = list(get_mongo_predictions(starttime, endtime, 10))
        with timer('Avro:  '):
            get_features(directory, file_name, predictions)
        endtime=starttime
