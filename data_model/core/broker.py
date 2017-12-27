# Dynamodb connection
# Author: Zex Li <top_zlynch@yahoo.com>
#
#\sa https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html
#from abc import abstractmethod
import ujson
import boto3
from boto3.dynamodb.conditions import Key, Attr
from data_model.dynamodb.common.config import get_config
from data_model.dynamodb.common.shared import GSI, ItemID, ItemType
from data_model.utils import get_boto3_service


class TableBroker(object):

    CHUNKSIZE = 256
    name = 'tbbrk'
    
    ITEMID_TABLETYPE = {getattr(ItemID, x):getattr(ItemType, x) for x in ItemID.__members__}

    def __init__(self, table_type):
        super(TableBroker, self).__init__()
        config = get_config()

        self._table_name = table_type
        self._table = get_dydb().Table(self._table_name)

    def __call__(self, **kwargs):
        return {k:getattr(self,k) for k in self.__slots__}

    def __str__(self):
        return ujson.dumps(self())

    def uniq_put(self, item, **kwargs):
        """
        Insert only if item not exists
        """
        if self.item_exists(item['source_unique']):
            return None
        return self.put(item)

    def put(self, item, **kwargs):
        """
        Put item into table

        Args:

        item: A `dict` object represents a record
        """
        return self._table.put_item(Item=item)

    def batch_put(self, items, **kwargs):
        with self._table.batch_writer() as batch:
            _ = [batch.put_item(Item=d) for d in items]

    def _get(self, key_dict, **kwargs):
        """
        Get item from table

        Args:

        key_dict: `dict` contains record identifier
        \code
        \endcode
        """
        return self._table.get_item(Key=key_dict)

    def _query(self, cond, ind=None, **kwargs):
        """
        Args:

        cond: ComparisonCondition object defined in [https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html]
        ind: A `string` indicates index name
        chunksize: Max number of items to return
        filter_expr: A ComparisonCondition object as filter expression
        esk: ExclusiveStartKey
        """
        if not cond and not ind:
            return None

        filter_expr = kwargs.get('filter_expr')
        chunksize = kwargs.get('chunksize', TableBroker.CHUNKSIZE)
        esk = kwargs.get('esk')

        kw = {
            'KeyConditionExpression':cond,
            'Limit':chunksize,
        }

        if ind:
            kw.update({'IndexName': ind})
        if esk:
            kw.update({'ExclusiveStartKey': esk})
        if filter_expr:
            kw.update({'FilterExpression': filter_expr})
        return self._table.query(**kw)

    def _delete(self, key_dict, **kwargs):
        """
        Args:

        key_dict: List of key dicts, eg.
        \code
        \endcode
        """
        if not key_dict:
            return None
        return self._table.delete_item(Key=key_dict)

    def batch_delete(self, key_dicts, **kwargs):
        """
        Args:

        key_dict: List of key dicts, eg.
        \code
            [
                {'id': 'ooooo'},
                ...
            ]
        \endcode
        """
        if not key_dicts:
            return None
        with self._table.batch_writer() as batch:
            _ = [batch.delete_item(Key=item) for item in key_dicts]

    def _update(self, key_dict, update_dict, **kwargs):
        """
        Update values defined in `update_dict` where match condition defined in `key_dict`
        Args:

        key_dict: `dict` contains `id`, eg.
        \code
        \endcode
        update_dict: `dict` contains key-value pairs to be updated, eg.
        \code
                {'name': 'human-1', 'location': 'mars'}
        \endcode
        """
        expr, cnt = 'SET ', 0
        name_dict, value_dict = {}, {}

        for k, v in update_dict.items():
            name_dict.update({'#k{}'.format(cnt): k})
            value_dict.update({':v{}'.format(cnt): v})
            cnt += 1

        expr += ', '.join(['{}={}'.format(k, v) for k, v in zip(name_dict, value_dict)])

        return self._table.update_item(
                Key=key_dict,
                UpdateExpression=expr,
                ExpressionAttributeNames=name_dict,
                ExpressionAttributeValues=value_dict,
                ReturnValues="ALL_NEW",
                )

    def _scan(self, filter_expr=None, **kwargs):
        """
        Scan table
        Args:

        filter_expr: ComparisonCondition object defined in [https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html]
        chunksize: Max number of results to scan, Table.CHUNKSIZE by default
        esk: ExclusiveStartKey
        """
        chunksize = kwargs.get('chunksize', TableBroker.CHUNKSIZE)
        esk = kwargs.get('esk')

        kw = {'Limit': chunksize}

        if filter_expr:
            kw.update({'FilterExpression': filter_expr})
        if esk:
            kw.update({'ExclusiveStartKey': esk})

        return self._table.scan(**kw)

    def _iscan(self, filter_expr=None, chunksize=1000, esk=None, **kwargs):
        """
        Scan table chunk by chunk

        Resume last scan by parsing previous `esk`

        Args:

        filter_expr: ComparisonCondition object defined in [https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html]
        chunksize: Max number of items to get for each scan
        esk: Exclusive start key

        @return Generator
        """
        if not esk:
            rsp = self._scan(filter_expr, chunksize=chunksize)
            if not rsp:
                return None

        while 'LastEvaluatedKey' in rsp:
            yield rsp
            esk = rsp['LastEvaluatedKey']
            rsp = self._scan(filter_expr, chunksize=chunksize, esk=esk)
        yield rsp

    def _iquery(self, cond, ind, filter_expr=None, chunksize=1000, esk=None, **kwargs):
        """
        Query table chunk by chunk

        Resume last scan by parsing previous `esk`

        Args:

        filter_expr: ComparisonCondition object defined in [https://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/conditions.html]
        chunksize: Max number of items to get for each scan
        esk: Exclusive start key

        @return Generator
        """
        if not esk:
            rsp = self._query(cond, ind, filter_expr=filter_expr, chunksize=chunksize)
            if not rsp:
                return None

        while 'LastEvaluatedKey' in rsp:
            yield rsp
            esk = rsp['LastEvaluatedKey']
            rsp = self._query(cond, ind, filter_expr=filter_expr, chunksize=chunksize, esk=esk)
        yield rsp

    def item_exists(self, source_unique):
        """
        Check item whether item with given `source_unique` exists

        Args:

        source_unique: A `string` represents the `source_unique` value
        """
        rsp = self._table.query(Key("source_unique").eq(source_unique), GSI.SOURCE_UNIQUE)
        return True if rsp['Items'] else False


    def _batch_get(self, keys, **kwargs):
        """
        Get items by given `keys`

        Args:
        keys: List of `dict` contains `npl_id` for each
        """
        if not keys:
            return None

        kw = {"Keys": keys}
        consist_read = kwargs.get('consist_read')

        if consist_read is not None:
            kw.update({'ConsistentRead': consist_read})

        kw = {'RequestItems': {self._table_name: kw}}
        return self._table.meta.client.batch_get_item(**kw)


def get_dydb():
    if not globals().get('dydb'):
        globals()['dydb'] = get_boto3_service('dynamodb')
    return globals().get('dydb')


def get_brk(item_id):
    tbtype = TableBroker.ITEMID_TABLETYPE[item_id]
    name = '{}:{}'.format(TableBroker.name, tbtype.value)
    if not globals().get(name):
        globals()[name] = TableBroker(tbtype.value)
    return globals().get(name)
