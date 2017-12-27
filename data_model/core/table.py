# Dynamodb connection
# Author: Zex Li <top_zlynch@yahoo.com>
#
#\sa https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html
#from abc import abstractmethod
import ujson
import uuid
from copy import deepcopy
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import Binary, Decimal
from data_model.dynamodb.common.shared import GSI, NAN, ItemID, ProcessStage
from data_model.dynamodb.common.utils import json_unzip, json_zip
from data_model.dynamodb.common.config import get_config
from data_model.dynamodb.core.broker import get_brk, TableBroker


class Table(object):

    SLICE_DELIM = '_'

    __slots__ = [
            "data_status",
            "source_unique",
            "data_type",
            "updated_ts",
            "updated_by",
            "created_ts",
            "created_by",
            "source",
            "s3_key",
    ]

    _fat_fields = ()

    @classmethod
    def init_dict(cls):
        now = datetime.now()
        return {
                "created_ts": int(now.timestamp()),
                "updated_ts": int(now.timestamp()),
                "data_status": ProcessStage.STAGE_1.name,
                }

    def initialize_default(self):
        init = type(self).init_dict()
        _ = [setattr(self, k, v) for k, v in init.items()]
        _ = [setattr(self, k, NAN) for k in Table.__slots__ if k not in init]
        _ = [setattr(self, k, None) for k in self.__slots__ if k not in init]

    def __str__(self):
        return ujson.dumps(self())

    def __init__(self, init_dict=None):
        if init_dict:
            _ = [setattr(self, k, init_dict.get(k, None)) for k in Table.__slots__]
            self._init_items(init_dict)
        else:
            self.initialize_default()

    @classmethod
    def rebuild(cls, **kwargs):
        """
        Rebuild objects by attributes

        Args:
        source_unique: A `string` indicates `source_unique` value for query
        id: A `string` indecates item identifier
        filter_expr: ComparisonCondition object defined in [https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html]
        """
        brk = get_brk(cls.ID)
        source_unique = kwargs.pop('source_unique', None)

        if source_unique:
            kwargs['cond'] = Key('source_unique').eq(source_unique)
            kwargs['ind'] = GSI.SOURCE_UNIQUE.value
        
        iid = kwargs.pop('id', None)
        if iid:
            kwargs['cond'] = Key(cls.ID.value).eq(iid)

        #cls.filter_data_type(kwargs)
        rsp = brk._query(**kwargs)
        items = rsp.get('Items')
        if not items:
            return [], []
        return cls.batch_get(items)

    @classmethod
    def iscan(cls, **kwargs):
        brk = get_brk(cls.ID)

        filter_expr = kwargs.pop('filter_expr', None)
        if filter_expr:
            filter_expr = Attr('data_type').eq(cls.DT.value) & filter_expr
        else:
            filter_expr = Attr('data_type').eq(cls.DT.value)

        for chunk in brk._iscan(filter_expr=filter_expr, **kwargs):
            yield cls.batch_build(chunk['Items'])

    @classmethod
    def iquery(cls, **kwargs):
        """
        Query chunk by chunk

        Args:

        verbose: If `True`, perform further `batch_get` for details,
                By default, False, return raw items in query results
        cond: Condition for query
        ind: Index name for query

        Reference to `Broker._query`
        """
        brk = get_brk(cls.ID)
        verbose = kwargs.pop('verbose', False)

        if kwargs.get('cond') is None:
            kwargs['cond'] = Key('data_type').eq(cls.DT)

        for chunk in brk._iquery(**kwargs):
            if verbose:
                yield cls.batch_get(chunk['Items'])
            else:
                yield chunk['Items']

    @classmethod
    def batch_get(cls, items, **kwargs):
        """
        Batch build object from given `dict` items
        Args:

        items: List of `dict` contains information to rebuild objects

        @return A list of `Table` objects
                A list of tuple indicates error items and correspoinding error
        """
        keys = cls.extract_key(items)
        rsp = get_brk(cls.ID)._batch_get(keys)

        if not rsp:
            return [], []
        items = rsp.get('Responses', {}).get(TableBroker.ITEMID_TABLETYPE[cls.ID].value)
        if not items:
            return [], []
        return cls.batch_build(items)

    @classmethod
    def batch_build(cls, items):
        """
        Batch build object from given `dict` items
        Args:

        items: List of `dict` contains information to rebuild objects

        @return A list of `Table` objects
                A list of tuple indicates error items and correspoinding error
        """
        objs = []
        err_items = []
        # Group items by item identifier
        for item in items:
            try:
                if not item:
                    continue
                obj = cls.foreach_item(item)
                if obj:
                    objs.append(obj)
            except Exception as ex:
                err_items.append((item, ex))
        return objs, err_items

    @classmethod
    def foreach_item(cls, item):
        def unzip(obj, f):
            val = getattr(obj, f, None)
            if val and isinstance(val, Binary):
                setattr(obj, f, json_unzip(val.value))

        def convert(obj, f):
            val = getattr(obj, f, None)
            if val and isinstance(val, Decimal):
                setattr(obj, f, int(str(val)))

        gid = item.get(cls.ID.value)
        if not gid:
            raise AttributeError("Invalid record without item id")
       
        obj = cls(item)
        if getattr(cls, '_fat_fields', None):
            list(map(lambda f:unzip(obj, f), cls._fat_fields))
        else:
            list(map(lambda f:unzip(obj, f), obj.__slots__))

        list(map(lambda f:convert(obj, f), obj().keys()))
        return obj

    @classmethod
    def delete(cls, **kwargs):
        """
        Delete by attributes

        Args:

        id: 
        batch: Wether perform batch delete, by default `True`
        Reference to `Broker._delete` and `Broker.batch_delete`

        Notes:
        - Either `id` must be provided
        """
        brk = get_brk(cls.ID)
        batch = kwargs.pop('batch', True)

        iid = kwargs.pop('id', None)
        if iid:
            brk._delete(iid)
            return

        filter_expr = kwargs.pop('filter_expr', None)
        source_unique = kwargs.pop('source_unique', None)
        if source_unique:
            kwargs['cond'] = Key('source_unique').eq(source_unique)
            kwargs['ind'] = GSI.SOURCE_UNIQUE.value

        cls.filter_data_type(kwargs)

        rsp = brk._query(**kwargs)
        items = rsp.get('Items')
        if not items:
            return
        items = cls.extract_key(items)

        if batch:
            return brk.batch_delete(items)
        return list(map(brk._delete, items))

    @classmethod
    def extract_key(cls, items):
        """
        Extract key group from items

        Args:
        items: List of `dict` contains key info
        """
        return list(map(lambda item: {cls.ID.value: item[cls.ID.value]}, items))

    @classmethod
    def filter_data_type(cls, kwargs):
        filter_expr = kwargs.pop('filter_expr', None)
        if filter_expr:
            filter_expr = filter_expr & Attr('data_type').eq(cls.DT.value)
        else:
            filter_expr = Attr('data_type').eq(cls.DT.value)
        kwargs.update({'filter_expr': filter_expr})

    def _init_items(self, items=None):
        """
        Make sure table field constrain data exists
        Initialize members with given items
        Args:

        items: A `dict` contains values for corresponding members
        """
        self._validate_constrain()
        if items:
            [setattr(self, k, items.get(k)) for k in self.__slots__]
        else:
            [setattr(self, k, None) for k in self.__slots__]

    def _validate_constrain(self):
        if not hasattr(self, '__slots__'):
            raise AttributeError("Table schema not defined")

    def __str__(self):
        return ujson.dumps(self())

    def __call__(self, **kwargs):
        val = {k:getattr(self,k) for k in self.__slots__ if hasattr(self, k)}
        val.update({k:getattr(self,k) for k in Table.__slots__ if hasattr(self, k)})
        return val

    def save(self, **kwargs):
        """
        Save global object
        To force split field, add field names to `Table._fat_fields`
        """
        in_place = kwargs.pop('in_place', False)
        obj = self if in_place else deepcopy(self)
        # Zip fat fields
        list(map(lambda ff: setattr(obj, ff, json_zip(\
                getattr(self, ff, None))), type(self)._fat_fields))
        obj.updated_ts = int(datetime.now().timestamp())
        # Put all slices for one item
        _ = [delattr(obj, f) for f in obj.__slots__ if None == getattr(obj, f, None)]
        get_brk(type(self).ID).put(obj())


class Repository(Table):

    ID = ItemID.REPOSITORY

    __slots__ = [
        ItemID.REPOSITORY.value,
        ]

    def __init__(self, init_dict=None):
        super(Repository, self).__init__(init_dict)
        if init_dict:
            _ = [setattr(self, k, init_dict.get(k, None)) for k in Repository.__slots__]
        else:
            setattr(self, ItemID.REPOSITORY.value, str(uuid.uuid1()))

    def __call__(self, **kwargs):
        val = super(Repository, self).__call__()
        val.update({k:getattr(self,k) for k in Repository.__slots__})
        return val
