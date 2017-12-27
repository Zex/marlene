# Core table selftest
# Author: Zex Li <top_zlynch@yahoo.com>
import sys
import gc
import pickle
import ujson
from boto3.dynamodb.conditions import Key, Attr
from data_model.dynamodb.common.shared import ChunkType
from data_model.dynamodb.core.global_table import Global
from data_model.dynamodb.tables.tech_offering import TechOffering
from data_model.dynamodb.tables.news import NewsTable
from data_model.dynamodb.tables.grant import GrantTable
from data_model.dynamodb.tables.organization import OrganizationTable
from data_model.dynamodb.tables.journal_article import JournalArticle
from datetime import datetime


def foreach_item(t):
    print('++ [i-gen] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts if t.updated_ts else 0), t.data))
    print(t.chunks)


def selftest_general():

    ts, _ = TechOffering.rebuild(source_unique='00UMS044')
    for t in ts:
        if not t.data.title:
            continue
        print('++ [rebuild] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.title))

    ts, _ = TechOffering.partial(source_unique='00UMS044', chunk_type_prefix=ChunkType.PRIMERY)
    for t in ts:
        print('++ [partial] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.title))

    ts, _ = NewsTable.rebuild(chunksize=10)
    for t in ts:
        print('++ [rebuild] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data))
        
    print(len(ts))

    ts, _ = GrantTable.rebuild(chunksize=10)
    for t in ts:
        print('++ [rebuild] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data))

    ts, _ = NewsTable.rebuild(global_id='dd222368-b620-11e7-8224-0242ac110005')
    for t in ts:
        print('++ [rebuild] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts if t.updated_ts else 0), t.data))
        
    print(len(ts), ts[0].chunks)

    ts, _ = NewsTable.partial(chunk_type_prefix='description', chunksize=20)
    for t in ts:
        print('++ [rebuild] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts if t.updated_ts else 0), t.data))
        
    print(len(ts))

    ts, _ = JournalArticle.rebuild(chunksize=2000)
    for t in ts:
        print('++ [rebuild] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts if t.updated_ts else 0), t.data))
        print(t.chunks)
        
    print(len(ts))


def selftest_iscan():
    for ts in JournalArticle.iscan(chunksize=100):
        list(map(foreach_item, ts))
        print(len(ts))

def selftest_iquery():
    for ts in JournalArticle.iquery(chunksize=10):
        list(map(foreach_item, ts))
        print(len(ts))


def selftest_save():
    mess = 'REMOVE LATER'
    # add mess
    #ts = JournalArticle.rebuild(chunksize=20)
    ts = JournalArticle.rebuild(source_unique='10.7554/eLife.00675')
    for t in ts:
        if not t.data.title:
            continue
        print('++ [rebuild] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.title))
        t.data.title += mess
        print(t.data.title)
        t.save()
        break

    # recover
    ts = JournalArticle.rebuild(source_unique='10.7554/eLife.00675')
    for t in ts:
        if not t.data.title:
            continue
        print('++ [rebuild] {} {} {} {}'.format(t.global_id, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.title))
        t.data.title = t.data.title.rstrip(mess)
        print(t.data.title)
        t.save()
        break


def selftest_chunks():
    def to_chunks():
        gl = Global()
        gl.data = TechOffering()
        gl.source_unique = 'Stanley-05'#'zzzex'
        gl.chunks = (ChunkType.PRIMERY, 'title')
        _ = [setattr(gl.data, k, '[TO] Initial commit {}'.format(k)) for k in gl.data.__slots__]
        gl.data.title = super_long
        gl.save()

    def from_chunks():
        ts, err_items = TechOffering.rebuild(filter_expre=Attr('source_unique').eq('Stanley-05'))
        for t in ts:
            if t.data.title is None:
                print('-- [rebuild] none title')
                continue
            print('++ [rebuild] {} {} {} {} {}'.format(t.global_id, t.data_type, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.summary))
            assert(len(t.data.title) == len(super_long))
            [print('++ [item] {} => {}'.format(k, getattr(t, k))) for k in t.__slots__ if k != 'data']
        gc.collect()
        print('++ [erritems] total {}'.format(len(err_items)))
        for e in err_items:
            [print('-- [erritems] {} => {}'.format(k, v)) for k, v in e.items() if k != 'data']

        gc.collect()
        for ts in TechOffering.iquery(chunksize=30, filter_expre=Attr('source_unique').eq('Stanley-05'), verbose=False):
            [print(t) for t in ts]
        rsp = TechOffering.delete(chunksize=10, source_unique='zzzex', batch=False)
        print(rsp)

    def build_partial():
        ts, err_items = TechOffering.partial('title')
        for t in ts:
            if t.data.title is None:
                print('-- [partial] none title')
                continue
            print('++ [partial] {} {} {} {} {}'.format(t.global_id, t.data_type, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.summary))
            assert(len(t.data.title) == len(super_long))

        for e in err_items:
            [print(k, v) for k, v in e.items() if k != 'data']
        print('++ [erritems] {}'.format(len(err_items)))

    def partial_save():
        ts, err_items = TechOffering.partial('title')
        for t in ts:
            if t.data.title is None:
                print('-- [partial] none title')
                continue
            print('++ [partial] {} {} {} {} {}'.format(t.global_id, t.data_type, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.summary))
            assert(len(t.data.title) == len(super_long))

        print('++ [erritems] {}'.format(len(err_items)))

        super_longee = "super long title is there" * 10000000
        ts, err_items = TechOffering.partial('title')
        for t in ts:
            if t.data.title is None:
                print('-- [partial] none title')
                continue
            print('++ [partial] {} {} {} {} {}'.format(t.global_id, t.data_type, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.summary))
            t.data.title = super_longee
            break

        for t in ts:
            if t.data.title is None:
                print('-- [partial] none title')
                continue
            print('++ [partial] {} {} {} {} {}'.format(t.global_id, t.data_type, t.source_unique, datetime.fromtimestamp(t.updated_ts), t.data.summary))
            assert(len(t.data.title) == len(super_long))
            print(t.data.title == super_longee)

        print('++ [erritems] {}'.format(len(err_items)))

    super_long = "super long title is here." * 10000000
    to_chunks()
    gc.collect()
    from_chunks()
    build_partial()
    partial_save()


def selftest_pickle():
    def dumpit():
        ts, _ = JournalArticle.rebuild(chunksize=100)
        data = [str(t) for t in ts]
        print('++ [pickle] length:{}'.format(len(ts)))
        with open(path, 'wb') as fd:
            pickle.dump(data, fd)

    def rebuild_global():
        with open(path, 'rb') as fd:
            objs = pickle.load(fd)
        for obj in objs:
            obj = Global(ujson.loads(obj))
            obj.data = JournalArticle(ujson.loads(obj.data))

    path = 'journal_article.pickle'
    dumpit()
    rebuild_global()


if __name__ == '__main__':
    # Uncomment first
    #selftest_general()
    #selftest_iscan()
    #selftest_iquery()
    #selftest_save()
    #selftest_chunks()
    #selftest_pickle()
