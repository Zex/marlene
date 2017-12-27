# Shared constants
# \sa https://confluence.zhihuiya.com/display/P3/Data+Schema
from enum import Enum, unique


@unique
class ProcessStage(Enum):
    STAGE_1 = "STAGE_1"


@unique
class GSI(Enum):
    SOURCE_UNIQUE = 'source_unique_index'


@unique
class ItemID(Enum):
    REPOSITORY = 'repository_id'

@unique
class ItemType(Enum):
    REPOSITORY = 'MARLENE_REPOSITORY'
    

MAX_SLICE_SIZE = 390 * 1024 # in bytes
NAN = "nan"
