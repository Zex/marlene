# Utilities go here
# Author: Zex Li <top_zlynch@yahoo.com>
import ujson
import gzip
import pickle
import sys


def clean_items(self, items, item):
    list(map(lambda item: items.pop(item), items))


def simple_zip(data):
    if not data:
        return None
    return gzip.compress(pickle.dumps(data))


def simple_unzip(data):
    if not data:
        return None
    return pickle.loads(gzip.decompress(data))


def json_zip(data):
    if not data:
        return None
    return gzip.compress(ujson.dumps(data).encode())


def json_unzip(data):
    if not data:
        return None
    return ujson.loads(gzip.decompress(data).decode())


def split_chunks(raw, max_chunk):
    """
    Split raw bytes into chunks in place

    Args:
    raw: Byte sequence
    max_chunk: Max size of each chunk
    """
    chunks = []
    end = max_chunk-sys.getsizeof(b'')

    while sys.getsizeof(raw) > max_chunk:
        chunks.append(raw[:end])
        raw = raw[end:]
    if sys.getsizeof(raw) > 0:
        chunks.append(raw)
        raw = []
    return chunks


def rebuild_chunks(chunks):
    """
    Rebuild raw bytes sequence from chunks in place

    Args:
    chunks: List of chunks
    """
    raw = b''
    while chunks:
        raw += chunks.pop(0)
    return raw
