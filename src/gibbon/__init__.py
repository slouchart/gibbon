"""
    Gibbon is an ETL framework in pure Python (3.7+)
    based around these ideas:

    1) the abstraction is a graph of tuple transformers, that graph is a DAG
    2) each transformer is either a source S, a target T, or an in-between transformation combining both T:S
    3) condition of validity is the existence of at least one path between any couple <S, T>
    4) mapping design and actual execution are decoupled
    5) the basic unit of processing is a tuple, namely a namedtuple with type enforcement (maybe not...)
    6) atomic datatypes are supported : int, float, str, bool, time, datetime.
    7) actual I/O handlers (databases, files, sockets, whatever) are selected at runtime
    8) actual execution mode (threaded, asynchronous, whatever) is selected at runtime
"""

from .workflows import *
from .io import *
from .execution import *





