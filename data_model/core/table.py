# Dynamodb connection
# Author: Zex Li <top_zlynch@yahoo.com>
#
#\sa https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html
#from abc import abstractmethod
import ujson
from boto3.dynamodb.conditions import Key, Attr
from data_model.dynamodb.common.shared import GSI, ChunkType
from data_model.dynamodb.common.utils import json_unzip
from data_model.dynamodb.common.config import get_config
from data_model.dynamodb.core.broker import get_brk
from data_model.dynamodb.core.global_table import Global


class Table(object):

    SLICE_DELIM = '_'

    @classmethod
    def rebuild(cls, **kwargs):
        """
        Rebuild objects by attributes

        Args:
        source_unique: A `string` indicates `source_unique` value for query
        global_id: A `string` indicates `global_id` value for query
        filter_expr: ComparisonCondition object defined in [https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html]
        """
        kw = {}
        brk = get_brk()
        source_unique = kwargs.pop('source_unique', None)
        global_id = kwargs.pop('global_id', None)

        if source_unique:
            kwargs['cond'] = Key('source_unique').eq(source_unique)
            kwargs['ind'] = GSI.SOURCE_UNIQUE
        elif global_id:
            kwargs['cond'] = Key('global_id').eq(global_id)
        else:
            kwargs['cond'] = Key('data_type').eq(cls.data_type)
            kwargs['ind'] = GSI.DATA_TYPE_UPDATE_TS

        rsp = brk._query(**kwargs)
        items = rsp.get('Items')
        if not items:
            return [], []
        return cls.batch_get(items)

    @classmethod
    def partial(cls, chunk_type_prefix, **kwargs):
        """
        Partial rebuild objects by attributes

        Args:
        source_unique: A `string` indicates `source_unique` value for query
        chunk_type_prefix: A `string` indecates field name of corresponding data type
        """
        kw = {}
        brk = get_brk()
        source_unique = kwargs.pop('source_unique', None)

        if source_unique:
            kwargs['cond'] = Key('source_unique').eq(source_unique)
            kwargs['ind'] = GSI.SOURCE_UNIQUE
        else:
            kwargs['cond'] = Key('data_type').eq(cls.data_type)
            kwargs['ind'] = GSI.DATA_TYPE_UPDATE_TS

        kwargs['filter_expr'] = Attr('chunk_type').begins_with(chunk_type_prefix)
        rsp = brk._query(**kwargs)
        items = rsp.get('Items')
        if not items:
            return [], []
        return cls.batch_get(items)

    @classmethod
    def iscan(cls, **kwargs):
        brk = get_brk()

        filter_expr = kwargs.pop('filter_expr', None)
        if filter_expr:
            filter_expr = Attr('data_type').eq(cls.data_type) & filter_expr
        else:
            filter_expr = Attr('data_type').eq(cls.data_type)

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
        brk = get_brk()
        verbose = kwargs.pop('verbose', False)

        if kwargs.get('cond') is None:
            kwargs['cond'] = Key('data_type').eq(cls.data_type)
            kwargs['ind'] = GSI.DATA_TYPE_UPDATE_TS

        for chunk in brk._iquery(**kwargs):
            if verbose:
                yield cls.batch_get(chunk['Items'])
            else:
                yield chunk['Items']

    @classmethod
    def batch_get(cls, items, **kwargs):
        keys = cls.extract_key(items)
        rsp = get_brk()._batch_get(keys)
        items = rsp.get('Responses', {}).get(get_config().table_global)
        if not items:
            return []
        return cls.batch_build(items)

    @classmethod
    def batch_build(cls, items):
        """
        Batch build object from given `dict` items
        Args:

        items: List of `dict` contains information to rebuild objects

        @return A list of `Global` objects
                A list of tuple indicates error items
        """
        objs, sty, mem = [], [], {}
        err_items = []
        # Group items by global_id
        for item in items:
            if not item:
                continue

            gid = item['global_id']
            grp = mem.get(gid, [])
            grp.append(item)
            mem[gid] = grp

        for gid in mem:
            items = mem[gid]
            try:
                obj = cls.build_each(items)
                if obj:
                    objs.append(obj)
            except Exception:
                err_items.extend(items)
        return objs, err_items

    @classmethod
    def build_each(cls, grp):
        """
        Build object from group of items
        Args:

        items: List of `dict` contains information to rebuild objects

        @return A `Global` object
        """
        if not grp:
            return None
        chunks = {}
        # Extract data from chunks
        grp = sorted(grp, key=lambda g:g['chunk_type'])
        for item in grp:
            sty = item['chunk_type']
            end = sty.rfind(cls.SLICE_DELIM)
            if end < 0:
                chunks[sty] = item['data'].value
            else:
                pref = sty[:end]
                field = chunks.get(pref, b'')
                field += item['data'].value
                chunks[pref] = field
        # All chunks share Global
        gl = Global(grp[0])
        gl.chunks = tuple(chunks.keys())

        if ChunkType.PRIMERY in chunks:
            gl.data = cls(json_unzip(chunks.pop(ChunkType.PRIMERY)))
        else:
            gl.data = cls()
        [setattr(gl.data, k, json_unzip(v)) for k, v in chunks.items()]
        return gl

    @classmethod
    def delete(cls, **kwargs):
        """
        Delete by attributes

        Args:

        batch: Wether perform batch delete, by default `True`
        global_id: Target item `global_id`
        chunk_type: Target item `chunk_type`
        Reference to `Broker._delete` and `Broker.batch_delete`
        """
        brk = get_brk()
        batch = kwargs.pop('batch', True)

        global_id = kwargs.pop('global_id', None)
        chunk_type = kwargs.pop('chunk_type', None)
        if global_id and chunk_type:
            brk._delete({'global_id':global_id, 'chunk_type': chunk_type})
            return

        filter_expr = kwargs.pop('filter_expr', None)
        source_unique = kwargs.pop('source_unique', None)
        if source_unique:
            kwargs['cond'] = Key('source_unique').eq(source_unique)
            kwargs['ind'] = GSI.SOURCE_UNIQUE
        else:
            kwargs['cond'] = Key('data_type').eq(cls.data_type)
            kwargs['ind'] = GSI.DATA_TYPE_UPDATE_TS

        if filter_expr:
            kwargs['filter_expr'] = filter_expr

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
        return list(map(lambda item: {
                'global_id': item['global_id'],\
                'chunk_type': item['chunk_type'],}, items))

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
        return {k:getattr(self,k) for k in self.__slots__}
