# The global table
# \sa ETL Data Schema https://confluence.zhihuiya.com/display/P3/ETL+Data+Schema
# Author: Zex Li <top_zlynch@yahoo.com>
import ujson
import sys
import uuid
from datetime import datetime
from data_model.dynamodb.common.utils import json_zip, split_chunks, rebuild_chunks, json_unzip
from data_model.dynamodb.common.shared import ProcessStage, ChunkType, MAX_SLICE_SIZE
from data_model.dynamodb.core.broker import TableBroker


class Global(TableBroker):

    @property
    def chunks(self):
        return self.__chunks

    @chunks.setter
    def chunks(self, val):
        if not isinstance(val, tuple):
            raise TypeError("Chunk name group requires tuple")
        self.__chunks = val

    __slots__ = [
            "global_id",
            "data_status",
            "chunk_type",
            "source_unique",
            "data_type",
            "updated_ts",
            "updated_by",
            "created_ts",
            "created_by",
            "source",
            "bucket_key",
            "data",
    ]

    def __init__(self, init_dict=None):
        super(Global, self).__init__()
        if init_dict:
            _ = [setattr(self, k, init_dict.get(k)) for k in self.__slots__]
        else:
            now = datetime.now()
            defval = "Initial commit"
            init_dict = {
                "global_id": str(uuid.uuid1()),
                "created_ts": int(now.timestamp()),
                "updated_ts": int(now.timestamp()),
                "chunk_type": ChunkType.PRIMARY,
                "data_status": ProcessStage.IMPORT.name.lower(),
                }
            _ = [setattr(self, k, defval) for k in self.__slots__ if k not in init_dict]
            _ = [setattr(self, k, v) for k, v in init_dict.items()]

    def __str__(self):
        gl = self()
        gl['data'] = str(gl['data'])
        return ujson.dumps(gl)

    @staticmethod
    def create(source_unique, details, chunk_type, data_type, updated_by=None, \
        created_by=None, source=None, data_status=ProcessStage.IMPORT, \
        bucket_key=None, **kwargs):
        """Create Global object from given parameters
        @return A `dict` object
        """

        now = datetime.now()
        return {
            "global_id": str(uuid.uuid1()),
            "data_status": data_status,
            "chunk_type": chunk_type,
            "source_unique": source_unique,
            "data_type": data_type,
            "updated_ts": int(now.timestamp()),
            "updated_by": updated_by,
            "created_ts": int(now.timestamp()),
            "created_by": created_by,
            "source": source,
            "bucket_key": bucket_key,
            "data": details,
        }

    @staticmethod
    def foreach_chunk(gl, sty, data):
        if not data:
            return []
        data = json_zip(data)
        gl.updated_ts = int(datetime.now().timestamp())
        chunks = split_chunks(data, MAX_SLICE_SIZE)
        for i, chunk in enumerate(chunks):
            gl.data = chunk
            gl.chunk_type = '{}_{}'.format(sty, i+1)
            yield gl

    def save(self, **kwargs):
        """
        Save global object
        To force split field, add field names to `gl.chunks`
        """
        chunks = []
        self.data_type = self.data.data_type
        # Extract non-primery chunk
        data = self.data
        self.data = b''
        for sty in self.chunks:
            if sty != ChunkType.PRIMARY:
                sli = getattr(data, sty)
                _ = [chunks.append(gl()) \
                        for gl in Global.foreach_chunk(Global(self()), sty, sli)]
                delattr(data, sty)

        if any([True for k in self.data.__slots__ if k in data]):
            # Add primery chunk
            self.data = json_zip(data)
            self.chunk_type = ChunkType.PRIMARY
            self.updated_ts = int(datetime.now().timestamp())
            chunks.insert(0, self())

        # Put all chunks for one item
        self.batch_put(chunks)
