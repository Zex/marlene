# Shared constants
from enum import Enum, unique


@unique
class ProcessStage(Enum):
    STAGE_1 = 0


class GSI(object):
    SOURCE_UNIQUE = 'source_unique_index'
    DATA_TYPE_UPDATE_TS = 'data_type_updated_ts_index'


class ChunkType(object):
    PRIMARY = 'primary'


MAX_SLICE_SIZE = 390 * 1024 # in bytes
