# Core table selftest
# Author: Zex Li <top_zlynch@yahoo.com>
import sys
import gc
import pickle
import ujson
from boto3.dynamodb.conditions import Key, Attr
from data_model.dynamodb.common.shared import NAN, GSI
from data_model.dynamodb.tables.tech_offering import TechOffering
from data_model.dynamodb.tables.news import NewsTable
from data_model.dynamodb.tables.grant import GrantTable
from data_model.dynamodb.tables.funded_research import FundedResearchTable
from data_model.dynamodb.tables.organization import OrganizationTable
from data_model.dynamodb.tables.journal_article import JournalArticle
from data_model.dynamodb.tables.tech_classi import TechClassi
from data_model.dynamodb.tables.industry_classi import IndustryClassi
from datetime import datetime


def foreach_item(t):
    if not t:
        return
    print('++ [i-gen] {} {} {} {}'.format(getattr(t, t.ID.value, None), \
            t.source_unique, \
            datetime.fromtimestamp(getattr(t, 'updated_ts', 0)),\
            getattr(t, 'title', NAN)))


def foreach_rebuild(t):
    if not t:
        return
    print('++ [rebuild] {} {} {} {}'.format(getattr(t, t.ID.value, None), \
            t.source_unique, \
            datetime.fromtimestamp(getattr(t, 'updated_ts', 0)),\
            getattr(t, 'title', NAN)))


def selftest_general():

    try:
        ts, _ = TechOffering.rebuild(source_unique='P12395')
        list(map(lambda t:foreach_rebuild(t), ts))
    except Exception as ex:
        print('-- [error]  {}'.format(ex))

    try:
        ts, _ = FundedResearchTable.rebuild(id='9001706a-b612-11e7-8e88-0242ac110002')
        list(map(lambda t:foreach_rebuild(t), ts))
    except Exception as ex:
        print('-- [error]  {}'.format(ex))


def selftest_classi():
    def gen_both():
        tgen =  TechClassi.iscan(chunksize=1000)
        igen = IndustryClassi.iscan(chunksize=1000)

        remain = 10
        for ts in tgen:
            yield list(map(lambda t:foreach_rebuild(t[0]) if t else NAN, ts))
            remain -= 1
            if not remain:
                break

        remain = 10
        for ts in igen:
            yield list(map(lambda t:foreach_rebuild(t[0]) if t else NAN, ts))
            remain -= 1
            if not remain:
                break

    list(gen_both())


def selftest_iscan():
    remain = 5000
    chunksize = 1000
    filter_expr = Attr('updated_ts').lt('1451577600')

    for ts, errs in GrantTable.iscan(chunksize=chunksize):
            #filter_expr=filter_expr):
        list(map(foreach_item, ts))
        remain -= chunksize
        print('++ [info] remain:{}'.format(remain))
        print('-- [error] errcnt:{} sample:{}'.format(len(errs), errs[0] if errs else NAN))
        if not remain:
            break


def selftest_iquery():
    remain = 5000
    chunksize = 1000
    filter_expr = Attr('updated_ts').gt('1451577600')
    cond = Key('source_unique').eq('28682')
    ind = GSI.SOURCE_UNIQUE.value
    
    for ts, errs in TechOffering.iquery(chunksize=chunksize, \
            cond=cond, ind=ind, verbose=True):
        list(map(lambda t:foreach_item(t), ts))
        remain -= chunksize
        print('++ [info] remain:{}'.format(remain))
        print('-- [error] errcnt:{} sample:{}'.format(len(errs), errs[0] if errs else NAN))
        if not remain:
            break


def selftest_save():
    ts = TechOffering()
    ts.repo_id = 'selftest_aaa'
    ts.save()
    print("++ [rebuild] {}".format(ts()))

    ts_rebuild, _ = TechOffering.rebuild(id=ts.repo_id)
    ts_rebuild = ts_rebuild[0]
    print("++ [rebuild] {}".format(ts_rebuild()))

    assert(ts() == ts_rebuild())

    ts_rebuild.title = 'good morning'
    ts_rebuild.save()

    ts_rebuild_again, _ = TechOffering.rebuild(id=ts.repo_id)
    ts_rebuild_again = ts_rebuild_again[0]
    print("++ [rebuild] {}".format(ts_rebuild_again()))

    assert(ts_rebuild() == ts_rebuild_again())

    ts_rebuild.save(in_place=True)
    assert(ts_rebuild() != ts_rebuild_again())


def selftest_resave():
    mess = 'REMOVE LATER'
    # add mess
    #ts = JournalArticle.rebuild(chunksize=20)
    ts = JournalArticle.rebuild(source_unique='10.7554/eLife.00675')
    for t in ts:
        if not t.title:
            continue
        foreach_rebuild(t)
        t.title += mess
        t.save()
        break

    # recover
    ts = JournalArticle.rebuild(source_unique='10.7554/eLife.00675')
    for t in ts:
        if not t.title:
            continue
        foreach_rebuild(t)
        t.title = t.title.rstrip(mess)
        t.save()
        break


if __name__ == '__main__':
    # Uncomment first
    #selftest_general()
    #selftest_classi()
    #selftest_iscan()
    #selftest_iquery()
    #selftest_save()
