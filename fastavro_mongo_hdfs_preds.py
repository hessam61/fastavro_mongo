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
    fields = []
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
    fields.append({
        'name': 'Prediction',
        'doc': 'Prediction type could be a classification or regression.',
        'type': 'float'
        })
    fields.append({
        'name': 'Score',
        'doc': 'Prediction type could be a classification, regression, or a string txt.',
        'type': 'float'
        })
    fields.append({
        'name': 'heuristic_rule',
        'doc': 'True or False to report the predicted outcome.  The Data Scientist can create heuristic rules that determines if the predictive is reported or not.  True=Report, False=Do not report.',
        'type': 'boolean'
        })

    return {
        'type': 'record',
        'name': 'Predictions',
        'doc': 'The transform is either scalar or vector (array). The ' +
            'indices are held in key value pairs in the schema.  The types ' +
            'of the values are unique to the transform component.  ' +
            'Typically the values will be floats.',
        'fields': fields
    }

def get_mongo_predictions(starttime, endtime, limit=None):
    q = {'modelName':'sepsismodel',
         'WCT':{'$gt':starttime,'$lt':endtime}}
    r = psPreds.find(q,{'WCT':1,'VISIT_NUMBER':1,'result':1,
        '_id':1})
    return r if limit is None else r.limit(limit)

def write_avro(file_name, predictions):
    avro_schema = generate_avro_schema()
    with AvroWriter(hdfs_client, file_name, schema=avro_schema, overwrite=True) as writer:
        for prediction in predictions:
            data = {}
            data['event_id'] = 'pred_' + str(prediction['_id'])
            data['valid_on'] = int(prediction['WCT'].timestamp())
            data['created_on'] = int(prediction['WCT'].timestamp())
            data['input_events'] = str(prediction['_id'])
            data['patient_id'] = prediction['VISIT_NUMBER']
            data['provenance'] = ['psPredsExtract', 'PredictSepsis']
            data['Prediction'] = float(prediction['result']['result']['predict'])
            data['Score'] = float(prediction['result']['result']['score'])
            data['heuristic_rule'] = prediction['result']['result']['heuristic_alert']
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

    for i in range(40):
        starttime = endtime - timedelta(1)
        file_name = ''.join(('preds/',
            starttime.strftime("%Y"), '/',
            starttime.strftime("%m"), '/',
            starttime.strftime("%Y_%m_%d"), '_transform.avro'))
        with timer('Mongo: '):
            predictions = list(get_mongo_predictions(starttime, endtime))
        with timer('Avro:  '):
            write_avro(file_name, predictions)
        endtime=starttime
