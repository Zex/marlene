# Core broker selftest
# Author: Zex Li <top_zlynch@yahoo.com>
import uuid
from datetime import datetime
from data_model.dynamodb.common.shared import GSI
from data_model.dynamodb.common.utils import json_zip, json_unzip
from data_model.dynamodb.core.broker import get_brk, Key, Attr
from data_model.dynamodb.tables.tech_offering import TechOffering

def selftest_general():

    now = datetime.now()
    item = {
        'global_id': 'selftest',
        'source_unique': 'zzzex',
        'updated_ts': int(now.timestamp()),
        'created_ts': int(now.timestamp()),
        'chunk_type': 'beyond primary',
        'data': json_zip({'name':'Amazing Project'}),
    }

    t = get_brk()

    rsp = t.put(item)
    print("++ [put] {}".format(rsp))
    rsp = t._query(Key('global_id').eq('selftest'))
    print("++ [query] {}".format(rsp))
    rsp = t._query(Key('source_unique').eq('zzzex'), 'source_unique_index')
    print("++ [query] {}".format(rsp))
    rsp = t._update(
            {'global_id': 'selftest', 'chunk_type': 'beyond_primary',},
            {'data': 'super amazing project'.encode()}
        )
    print("++ [update] {}".format(rsp))
    t.batch_delete([{'global_id': 'selftest', 'chunk_type': 'beyond primary'}])
    print("++ [delete] {}".format('done'))

    items = [{
                'global_id': str(uuid.uuid1()),
                'source_unique': 'zzzex',
                'updated_ts': int(now.timestamp()),
                'created_ts': int(now.timestamp()),
                'chunk_type': 'beyond primary',
                'data': 'Amazing Project'.encode(),
            } for _ in range(10)]

    def foreach_item():
        yield from items

    t.batch_put(foreach_item())
    print("++ [batch-put] {}".format('done'))
    rsp = t._scan(Attr('chunk_type').eq('beyond primary'))
    print("++ [scan] {}".format(rsp))
    assert(len(items)==rsp.get('Count'))
    for item in rsp['Items']:
        ret = t._get({'global_id': item['global_id'], 'chunk_type': item['chunk_type']})
        print("++ [get] {}".format(ret))
        ret = t._delete({'global_id': item['global_id'], 'chunk_type': item['chunk_type']})
        print("++ [delete] {}".format(ret))


def selftest_iscan():

    t = get_brk()
    expr = Attr('data_type').eq('journal_article') & Attr('chunk_type').begins_with('primary')

    for i in t._iscan(expr, chunksize=100):
        print(i)
        if not i.get('Items'):
            break
        print('='*30)
        print(len(i.get('Items')), i['Items'][-1].get('data_type'), i['Items'][-1].get('chunk_type'))
        for item in i.get('Items'):
            article = json_unzip(item['data'].value)
            if not article.get('abstract'):
                break
            print(article['abstract'])


def selftest_iquery():

    t = get_brk()
    expr = Attr('chunk_type').begins_with('primary')

    for i in t._iquery(Key('data_type').eq('journal_article'), GSI.DATA_TYPE_UPDATE_TS, filter_expr=expr, chunksize=100):
        if not i.get('Items'):
            break
        print('='*30)
        print(len(i.get('Items')), i['Items'][-1].get('data_type'), i['Items'][-1].get('chunk_type'))
        for item in i.get('Items'):
            rsp = t._get({'global_id': item['global_id'], 'chunk_type': item['chunk_type']})
            print(rsp)


if __name__ == '__main__':
    selftest_general()
    selftest_iscan()
    selftest_iquery()
