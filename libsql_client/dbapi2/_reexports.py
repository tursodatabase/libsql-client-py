from sqlite3.dbapi2 import adapt
from sqlite3.dbapi2 import adapters
from sqlite3.dbapi2 import apilevel
from sqlite3.dbapi2 import Binary
from sqlite3.dbapi2 import complete_statement
from sqlite3.dbapi2 import converters
from sqlite3.dbapi2 import DatabaseError
from sqlite3.dbapi2 import DataError
from sqlite3.dbapi2 import Date
from sqlite3.dbapi2 import DateFromTicks
from sqlite3.dbapi2 import Error
from sqlite3.dbapi2 import IntegrityError
from sqlite3.dbapi2 import InterfaceError
from sqlite3.dbapi2 import InternalError
from sqlite3.dbapi2 import NotSupportedError
from sqlite3.dbapi2 import OperationalError
from sqlite3.dbapi2 import paramstyle
from sqlite3.dbapi2 import PARSE_COLNAMES
from sqlite3.dbapi2 import PARSE_DECLTYPES
from sqlite3.dbapi2 import PrepareProtocol
from sqlite3.dbapi2 import ProgrammingError
from sqlite3.dbapi2 import register_adapter
from sqlite3.dbapi2 import register_converter
from sqlite3.dbapi2 import SQLITE_ALTER_TABLE
from sqlite3.dbapi2 import SQLITE_ANALYZE
from sqlite3.dbapi2 import SQLITE_ATTACH
from sqlite3.dbapi2 import SQLITE_CREATE_INDEX
from sqlite3.dbapi2 import SQLITE_CREATE_TABLE
from sqlite3.dbapi2 import SQLITE_CREATE_TEMP_INDEX
from sqlite3.dbapi2 import SQLITE_CREATE_TEMP_TABLE
from sqlite3.dbapi2 import SQLITE_CREATE_TEMP_TRIGGER
from sqlite3.dbapi2 import SQLITE_CREATE_TEMP_VIEW
from sqlite3.dbapi2 import SQLITE_CREATE_TRIGGER
from sqlite3.dbapi2 import SQLITE_CREATE_VIEW
from sqlite3.dbapi2 import SQLITE_CREATE_VTABLE
from sqlite3.dbapi2 import SQLITE_DELETE
from sqlite3.dbapi2 import SQLITE_DENY
from sqlite3.dbapi2 import SQLITE_DETACH
from sqlite3.dbapi2 import SQLITE_DONE
from sqlite3.dbapi2 import SQLITE_DROP_INDEX
from sqlite3.dbapi2 import SQLITE_DROP_TABLE
from sqlite3.dbapi2 import SQLITE_DROP_TEMP_INDEX
from sqlite3.dbapi2 import SQLITE_DROP_TEMP_TABLE
from sqlite3.dbapi2 import SQLITE_DROP_TEMP_TRIGGER
from sqlite3.dbapi2 import SQLITE_DROP_TEMP_VIEW
from sqlite3.dbapi2 import SQLITE_DROP_TRIGGER
from sqlite3.dbapi2 import SQLITE_DROP_VIEW
from sqlite3.dbapi2 import SQLITE_DROP_VTABLE
from sqlite3.dbapi2 import SQLITE_FUNCTION
from sqlite3.dbapi2 import SQLITE_IGNORE
from sqlite3.dbapi2 import SQLITE_INSERT
from sqlite3.dbapi2 import SQLITE_OK
from sqlite3.dbapi2 import SQLITE_PRAGMA
from sqlite3.dbapi2 import SQLITE_READ
from sqlite3.dbapi2 import SQLITE_RECURSIVE
from sqlite3.dbapi2 import SQLITE_REINDEX
from sqlite3.dbapi2 import SQLITE_SAVEPOINT
from sqlite3.dbapi2 import SQLITE_SELECT
from sqlite3.dbapi2 import SQLITE_TRANSACTION
from sqlite3.dbapi2 import SQLITE_UPDATE
from sqlite3.dbapi2 import sqlite_version
from sqlite3.dbapi2 import sqlite_version_info
from sqlite3.dbapi2 import threadsafety
from sqlite3.dbapi2 import Time
from sqlite3.dbapi2 import TimeFromTicks
from sqlite3.dbapi2 import Timestamp
from sqlite3.dbapi2 import TimestampFromTicks
from sqlite3.dbapi2 import version
from sqlite3.dbapi2 import Warning
import sys

if sys.version_info[:2] >= (3, 11):
    from sqlite3.dbapi2 import Blob
    from sqlite3.dbapi2 import SQLITE_ABORT
    from sqlite3.dbapi2 import SQLITE_ABORT_ROLLBACK
    from sqlite3.dbapi2 import SQLITE_AUTH_USER
    from sqlite3.dbapi2 import SQLITE_AUTH
    from sqlite3.dbapi2 import SQLITE_BUSY_RECOVERY
    from sqlite3.dbapi2 import SQLITE_BUSY_SNAPSHOT
    from sqlite3.dbapi2 import SQLITE_BUSY_TIMEOUT
    from sqlite3.dbapi2 import SQLITE_BUSY
    from sqlite3.dbapi2 import SQLITE_CANTOPEN_CONVPATH
    from sqlite3.dbapi2 import SQLITE_CANTOPEN_DIRTYWAL
    from sqlite3.dbapi2 import SQLITE_CANTOPEN_FULLPATH
    from sqlite3.dbapi2 import SQLITE_CANTOPEN_ISDIR
    from sqlite3.dbapi2 import SQLITE_CANTOPEN_NOTEMPDIR
    from sqlite3.dbapi2 import SQLITE_CANTOPEN_SYMLINK
    from sqlite3.dbapi2 import SQLITE_CANTOPEN
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_CHECK
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_COMMITHOOK
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_FOREIGNKEY
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_FUNCTION
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_NOTNULL
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_PINNED
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_PRIMARYKEY
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_ROWID
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_TRIGGER
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_UNIQUE
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT_VTAB
    from sqlite3.dbapi2 import SQLITE_CONSTRAINT
    from sqlite3.dbapi2 import SQLITE_CORRUPT_INDEX
    from sqlite3.dbapi2 import SQLITE_CORRUPT_SEQUENCE
    from sqlite3.dbapi2 import SQLITE_CORRUPT_VTAB
    from sqlite3.dbapi2 import SQLITE_CORRUPT
    from sqlite3.dbapi2 import SQLITE_EMPTY
    from sqlite3.dbapi2 import SQLITE_ERROR_MISSING_COLLSEQ
    from sqlite3.dbapi2 import SQLITE_ERROR_RETRY
    from sqlite3.dbapi2 import SQLITE_ERROR_SNAPSHOT
    from sqlite3.dbapi2 import SQLITE_ERROR
    from sqlite3.dbapi2 import SQLITE_FORMAT
    from sqlite3.dbapi2 import SQLITE_FULL
    from sqlite3.dbapi2 import SQLITE_INTERNAL
    from sqlite3.dbapi2 import SQLITE_INTERRUPT
    from sqlite3.dbapi2 import SQLITE_IOERR_ACCESS
    from sqlite3.dbapi2 import SQLITE_IOERR_AUTH
    from sqlite3.dbapi2 import SQLITE_IOERR_BEGIN_ATOMIC
    from sqlite3.dbapi2 import SQLITE_IOERR_BLOCKED
    from sqlite3.dbapi2 import SQLITE_IOERR_CHECKRESERVEDLOCK
    from sqlite3.dbapi2 import SQLITE_IOERR_CLOSE
    from sqlite3.dbapi2 import SQLITE_IOERR_COMMIT_ATOMIC
    from sqlite3.dbapi2 import SQLITE_IOERR_CONVPATH
    from sqlite3.dbapi2 import SQLITE_IOERR_CORRUPTFS
    from sqlite3.dbapi2 import SQLITE_IOERR_DATA
    from sqlite3.dbapi2 import SQLITE_IOERR_DELETE_NOENT
    from sqlite3.dbapi2 import SQLITE_IOERR_DELETE
    from sqlite3.dbapi2 import SQLITE_IOERR_DIR_CLOSE
    from sqlite3.dbapi2 import SQLITE_IOERR_DIR_FSYNC
    from sqlite3.dbapi2 import SQLITE_IOERR_FSTAT
    from sqlite3.dbapi2 import SQLITE_IOERR_FSYNC
    from sqlite3.dbapi2 import SQLITE_IOERR_GETTEMPPATH
    from sqlite3.dbapi2 import SQLITE_IOERR_LOCK
    from sqlite3.dbapi2 import SQLITE_IOERR_MMAP
    from sqlite3.dbapi2 import SQLITE_IOERR_NOMEM
    from sqlite3.dbapi2 import SQLITE_IOERR_RDLOCK
    from sqlite3.dbapi2 import SQLITE_IOERR_READ
    from sqlite3.dbapi2 import SQLITE_IOERR_ROLLBACK_ATOMIC
    from sqlite3.dbapi2 import SQLITE_IOERR_SEEK
    from sqlite3.dbapi2 import SQLITE_IOERR_SHMLOCK
    from sqlite3.dbapi2 import SQLITE_IOERR_SHMMAP
    from sqlite3.dbapi2 import SQLITE_IOERR_SHMOPEN
    from sqlite3.dbapi2 import SQLITE_IOERR_SHMSIZE
    from sqlite3.dbapi2 import SQLITE_IOERR_SHORT_READ
    from sqlite3.dbapi2 import SQLITE_IOERR_TRUNCATE
    from sqlite3.dbapi2 import SQLITE_IOERR_UNLOCK
    from sqlite3.dbapi2 import SQLITE_IOERR_VNODE
    from sqlite3.dbapi2 import SQLITE_IOERR_WRITE
    from sqlite3.dbapi2 import SQLITE_IOERR
    from sqlite3.dbapi2 import SQLITE_LIMIT_ATTACHED
    from sqlite3.dbapi2 import SQLITE_LIMIT_COLUMN
    from sqlite3.dbapi2 import SQLITE_LIMIT_COMPOUND_SELECT
    from sqlite3.dbapi2 import SQLITE_LIMIT_EXPR_DEPTH
    from sqlite3.dbapi2 import SQLITE_LIMIT_FUNCTION_ARG
    from sqlite3.dbapi2 import SQLITE_LIMIT_LENGTH
    from sqlite3.dbapi2 import SQLITE_LIMIT_LIKE_PATTERN_LENGTH
    from sqlite3.dbapi2 import SQLITE_LIMIT_SQL_LENGTH
    from sqlite3.dbapi2 import SQLITE_LIMIT_TRIGGER_DEPTH
    from sqlite3.dbapi2 import SQLITE_LIMIT_VARIABLE_NUMBER
    from sqlite3.dbapi2 import SQLITE_LIMIT_VDBE_OP
    from sqlite3.dbapi2 import SQLITE_LIMIT_WORKER_THREADS
    from sqlite3.dbapi2 import SQLITE_LOCKED_SHAREDCACHE
    from sqlite3.dbapi2 import SQLITE_LOCKED_VTAB
    from sqlite3.dbapi2 import SQLITE_LOCKED
    from sqlite3.dbapi2 import SQLITE_MISMATCH
    from sqlite3.dbapi2 import SQLITE_MISUSE
    from sqlite3.dbapi2 import SQLITE_NOLFS
    from sqlite3.dbapi2 import SQLITE_NOMEM
    from sqlite3.dbapi2 import SQLITE_NOTADB
    from sqlite3.dbapi2 import SQLITE_NOTFOUND
    from sqlite3.dbapi2 import SQLITE_NOTICE_RECOVER_ROLLBACK
    from sqlite3.dbapi2 import SQLITE_NOTICE_RECOVER_WAL
    from sqlite3.dbapi2 import SQLITE_NOTICE
    from sqlite3.dbapi2 import SQLITE_OK_LOAD_PERMANENTLY
    from sqlite3.dbapi2 import SQLITE_OK_SYMLINK
    from sqlite3.dbapi2 import SQLITE_PERM
    from sqlite3.dbapi2 import SQLITE_PROTOCOL
    from sqlite3.dbapi2 import SQLITE_RANGE
    from sqlite3.dbapi2 import SQLITE_READONLY_CANTINIT
    from sqlite3.dbapi2 import SQLITE_READONLY_CANTLOCK
    from sqlite3.dbapi2 import SQLITE_READONLY_DBMOVED
    from sqlite3.dbapi2 import SQLITE_READONLY_DIRECTORY
    from sqlite3.dbapi2 import SQLITE_READONLY_RECOVERY
    from sqlite3.dbapi2 import SQLITE_READONLY_ROLLBACK
    from sqlite3.dbapi2 import SQLITE_READONLY
    from sqlite3.dbapi2 import SQLITE_ROW
    from sqlite3.dbapi2 import SQLITE_SCHEMA
    from sqlite3.dbapi2 import SQLITE_TOOBIG
    from sqlite3.dbapi2 import SQLITE_WARNING_AUTOINDEX
    from sqlite3.dbapi2 import SQLITE_WARNING
else:

    class Blob:
        pass

    # Most constants were only defined in 3.11
    #
    # you can generate them by using getattr(sqlite3.dbapi2, name)
    # in a newer python version
    SQLITE_ABORT = 4
    SQLITE_ABORT_ROLLBACK = 516
    SQLITE_AUTH_USER = 279
    SQLITE_AUTH = 23
    SQLITE_BUSY_RECOVERY = 261
    SQLITE_BUSY_SNAPSHOT = 517
    SQLITE_BUSY_TIMEOUT = 773
    SQLITE_BUSY = 5
    SQLITE_CANTOPEN_CONVPATH = 1038
    SQLITE_CANTOPEN_DIRTYWAL = 1294
    SQLITE_CANTOPEN_FULLPATH = 782
    SQLITE_CANTOPEN_ISDIR = 526
    SQLITE_CANTOPEN_NOTEMPDIR = 270
    SQLITE_CANTOPEN_SYMLINK = 1550
    SQLITE_CANTOPEN = 14
    SQLITE_CONSTRAINT_CHECK = 275
    SQLITE_CONSTRAINT_COMMITHOOK = 531
    SQLITE_CONSTRAINT_FOREIGNKEY = 787
    SQLITE_CONSTRAINT_FUNCTION = 1043
    SQLITE_CONSTRAINT_NOTNULL = 1299
    SQLITE_CONSTRAINT_PINNED = 2835
    SQLITE_CONSTRAINT_PRIMARYKEY = 1555
    SQLITE_CONSTRAINT_ROWID = 2579
    SQLITE_CONSTRAINT_TRIGGER = 1811
    SQLITE_CONSTRAINT_UNIQUE = 2067
    SQLITE_CONSTRAINT_VTAB = 2323
    SQLITE_CONSTRAINT = 19
    SQLITE_CORRUPT_INDEX = 779
    SQLITE_CORRUPT_SEQUENCE = 523
    SQLITE_CORRUPT_VTAB = 267
    SQLITE_CORRUPT = 11
    SQLITE_EMPTY = 16
    SQLITE_ERROR_MISSING_COLLSEQ = 257
    SQLITE_ERROR_RETRY = 513
    SQLITE_ERROR_SNAPSHOT = 769
    SQLITE_ERROR = 1
    SQLITE_FORMAT = 24
    SQLITE_FULL = 13
    SQLITE_INTERNAL = 2
    SQLITE_INTERRUPT = 9
    SQLITE_IOERR_ACCESS = 3338
    SQLITE_IOERR_AUTH = 7178
    SQLITE_IOERR_BEGIN_ATOMIC = 7434
    SQLITE_IOERR_BLOCKED = 2826
    SQLITE_IOERR_CHECKRESERVEDLOCK = 3594
    SQLITE_IOERR_CLOSE = 4106
    SQLITE_IOERR_COMMIT_ATOMIC = 7690
    SQLITE_IOERR_CONVPATH = 6666
    SQLITE_IOERR_CORRUPTFS = 8458
    SQLITE_IOERR_DATA = 8202
    SQLITE_IOERR_DELETE_NOENT = 5898
    SQLITE_IOERR_DELETE = 2570
    SQLITE_IOERR_DIR_CLOSE = 4362
    SQLITE_IOERR_DIR_FSYNC = 1290
    SQLITE_IOERR_FSTAT = 1802
    SQLITE_IOERR_FSYNC = 1034
    SQLITE_IOERR_GETTEMPPATH = 6410
    SQLITE_IOERR_LOCK = 3850
    SQLITE_IOERR_MMAP = 6154
    SQLITE_IOERR_NOMEM = 3082
    SQLITE_IOERR_RDLOCK = 2314
    SQLITE_IOERR_READ = 266
    SQLITE_IOERR_ROLLBACK_ATOMIC = 7946
    SQLITE_IOERR_SEEK = 5642
    SQLITE_IOERR_SHMLOCK = 5130
    SQLITE_IOERR_SHMMAP = 5386
    SQLITE_IOERR_SHMOPEN = 4618
    SQLITE_IOERR_SHMSIZE = 4874
    SQLITE_IOERR_SHORT_READ = 522
    SQLITE_IOERR_TRUNCATE = 1546
    SQLITE_IOERR_UNLOCK = 2058
    SQLITE_IOERR_VNODE = 6922
    SQLITE_IOERR_WRITE = 778
    SQLITE_IOERR = 10
    SQLITE_LIMIT_ATTACHED = 7
    SQLITE_LIMIT_COLUMN = 2
    SQLITE_LIMIT_COMPOUND_SELECT = 4
    SQLITE_LIMIT_EXPR_DEPTH = 3
    SQLITE_LIMIT_FUNCTION_ARG = 6
    SQLITE_LIMIT_LENGTH = 0
    SQLITE_LIMIT_LIKE_PATTERN_LENGTH = 8
    SQLITE_LIMIT_SQL_LENGTH = 1
    SQLITE_LIMIT_TRIGGER_DEPTH = 10
    SQLITE_LIMIT_VARIABLE_NUMBER = 9
    SQLITE_LIMIT_VDBE_OP = 5
    SQLITE_LIMIT_WORKER_THREADS = 11
    SQLITE_LOCKED_SHAREDCACHE = 262
    SQLITE_LOCKED_VTAB = 518
    SQLITE_LOCKED = 6
    SQLITE_MISMATCH = 20
    SQLITE_MISUSE = 21
    SQLITE_NOLFS = 22
    SQLITE_NOMEM = 7
    SQLITE_NOTADB = 26
    SQLITE_NOTFOUND = 12
    SQLITE_NOTICE_RECOVER_ROLLBACK = 539
    SQLITE_NOTICE_RECOVER_WAL = 283
    SQLITE_NOTICE = 27
    SQLITE_OK_LOAD_PERMANENTLY = 256
    SQLITE_OK_SYMLINK = 512
    SQLITE_PERM = 3
    SQLITE_PROTOCOL = 15
    SQLITE_RANGE = 25
    SQLITE_READONLY_CANTINIT = 1288
    SQLITE_READONLY_CANTLOCK = 520
    SQLITE_READONLY_DBMOVED = 1032
    SQLITE_READONLY_DIRECTORY = 1544
    SQLITE_READONLY_RECOVERY = 264
    SQLITE_READONLY_ROLLBACK = 776
    SQLITE_READONLY = 8
    SQLITE_ROW = 100
    SQLITE_SCHEMA = 17
    SQLITE_TOOBIG = 18
    SQLITE_WARNING_AUTOINDEX = 284
    SQLITE_WARNING = 28


__all__ = (
    "adapt",
    "adapters",
    "apilevel",
    "Binary",
    "Blob",
    "complete_statement",
    "converters",
    "DatabaseError",
    "DataError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "paramstyle",
    "PARSE_COLNAMES",
    "PARSE_DECLTYPES",
    "PrepareProtocol",
    "ProgrammingError",
    "register_adapter",
    "register_converter",
    "SQLITE_ABORT_ROLLBACK",
    "SQLITE_ABORT",
    "SQLITE_ALTER_TABLE",
    "SQLITE_ANALYZE",
    "SQLITE_ATTACH",
    "SQLITE_AUTH_USER",
    "SQLITE_AUTH",
    "SQLITE_BUSY_RECOVERY",
    "SQLITE_BUSY_SNAPSHOT",
    "SQLITE_BUSY_TIMEOUT",
    "SQLITE_BUSY",
    "SQLITE_CANTOPEN_CONVPATH",
    "SQLITE_CANTOPEN_DIRTYWAL",
    "SQLITE_CANTOPEN_FULLPATH",
    "SQLITE_CANTOPEN_ISDIR",
    "SQLITE_CANTOPEN_NOTEMPDIR",
    "SQLITE_CANTOPEN_SYMLINK",
    "SQLITE_CANTOPEN",
    "SQLITE_CONSTRAINT_CHECK",
    "SQLITE_CONSTRAINT_COMMITHOOK",
    "SQLITE_CONSTRAINT_FOREIGNKEY",
    "SQLITE_CONSTRAINT_FUNCTION",
    "SQLITE_CONSTRAINT_NOTNULL",
    "SQLITE_CONSTRAINT_PINNED",
    "SQLITE_CONSTRAINT_PRIMARYKEY",
    "SQLITE_CONSTRAINT_ROWID",
    "SQLITE_CONSTRAINT_TRIGGER",
    "SQLITE_CONSTRAINT_UNIQUE",
    "SQLITE_CONSTRAINT_VTAB",
    "SQLITE_CONSTRAINT",
    "SQLITE_CORRUPT_INDEX",
    "SQLITE_CORRUPT_SEQUENCE",
    "SQLITE_CORRUPT_VTAB",
    "SQLITE_CORRUPT",
    "SQLITE_CREATE_INDEX",
    "SQLITE_CREATE_TABLE",
    "SQLITE_CREATE_TEMP_INDEX",
    "SQLITE_CREATE_TEMP_TABLE",
    "SQLITE_CREATE_TEMP_TRIGGER",
    "SQLITE_CREATE_TEMP_VIEW",
    "SQLITE_CREATE_TRIGGER",
    "SQLITE_CREATE_VIEW",
    "SQLITE_CREATE_VTABLE",
    "SQLITE_DELETE",
    "SQLITE_DENY",
    "SQLITE_DETACH",
    "SQLITE_DONE",
    "SQLITE_DROP_INDEX",
    "SQLITE_DROP_TABLE",
    "SQLITE_DROP_TEMP_INDEX",
    "SQLITE_DROP_TEMP_TABLE",
    "SQLITE_DROP_TEMP_TRIGGER",
    "SQLITE_DROP_TEMP_VIEW",
    "SQLITE_DROP_TRIGGER",
    "SQLITE_DROP_VIEW",
    "SQLITE_DROP_VTABLE",
    "SQLITE_EMPTY",
    "SQLITE_ERROR_MISSING_COLLSEQ",
    "SQLITE_ERROR_RETRY",
    "SQLITE_ERROR_SNAPSHOT",
    "SQLITE_ERROR",
    "SQLITE_FORMAT",
    "SQLITE_FULL",
    "SQLITE_FUNCTION",
    "SQLITE_IGNORE",
    "SQLITE_INSERT",
    "SQLITE_INTERNAL",
    "SQLITE_INTERRUPT",
    "SQLITE_IOERR_ACCESS",
    "SQLITE_IOERR_AUTH",
    "SQLITE_IOERR_BEGIN_ATOMIC",
    "SQLITE_IOERR_BLOCKED",
    "SQLITE_IOERR_CHECKRESERVEDLOCK",
    "SQLITE_IOERR_CLOSE",
    "SQLITE_IOERR_COMMIT_ATOMIC",
    "SQLITE_IOERR_CONVPATH",
    "SQLITE_IOERR_CORRUPTFS",
    "SQLITE_IOERR_DATA",
    "SQLITE_IOERR_DELETE_NOENT",
    "SQLITE_IOERR_DELETE",
    "SQLITE_IOERR_DIR_CLOSE",
    "SQLITE_IOERR_DIR_FSYNC",
    "SQLITE_IOERR_FSTAT",
    "SQLITE_IOERR_FSYNC",
    "SQLITE_IOERR_GETTEMPPATH",
    "SQLITE_IOERR_LOCK",
    "SQLITE_IOERR_MMAP",
    "SQLITE_IOERR_NOMEM",
    "SQLITE_IOERR_RDLOCK",
    "SQLITE_IOERR_READ",
    "SQLITE_IOERR_ROLLBACK_ATOMIC",
    "SQLITE_IOERR_SEEK",
    "SQLITE_IOERR_SHMLOCK",
    "SQLITE_IOERR_SHMMAP",
    "SQLITE_IOERR_SHMOPEN",
    "SQLITE_IOERR_SHMSIZE",
    "SQLITE_IOERR_SHORT_READ",
    "SQLITE_IOERR_TRUNCATE",
    "SQLITE_IOERR_UNLOCK",
    "SQLITE_IOERR_VNODE",
    "SQLITE_IOERR_WRITE",
    "SQLITE_IOERR",
    "SQLITE_LIMIT_ATTACHED",
    "SQLITE_LIMIT_COLUMN",
    "SQLITE_LIMIT_COMPOUND_SELECT",
    "SQLITE_LIMIT_EXPR_DEPTH",
    "SQLITE_LIMIT_FUNCTION_ARG",
    "SQLITE_LIMIT_LENGTH",
    "SQLITE_LIMIT_LIKE_PATTERN_LENGTH",
    "SQLITE_LIMIT_SQL_LENGTH",
    "SQLITE_LIMIT_TRIGGER_DEPTH",
    "SQLITE_LIMIT_VARIABLE_NUMBER",
    "SQLITE_LIMIT_VDBE_OP",
    "SQLITE_LIMIT_WORKER_THREADS",
    "SQLITE_LOCKED_SHAREDCACHE",
    "SQLITE_LOCKED_VTAB",
    "SQLITE_LOCKED",
    "SQLITE_MISMATCH",
    "SQLITE_MISUSE",
    "SQLITE_NOLFS",
    "SQLITE_NOMEM",
    "SQLITE_NOTADB",
    "SQLITE_NOTFOUND",
    "SQLITE_NOTICE_RECOVER_ROLLBACK",
    "SQLITE_NOTICE_RECOVER_WAL",
    "SQLITE_NOTICE",
    "SQLITE_OK_LOAD_PERMANENTLY",
    "SQLITE_OK_SYMLINK",
    "SQLITE_OK",
    "SQLITE_PERM",
    "SQLITE_PRAGMA",
    "SQLITE_PROTOCOL",
    "SQLITE_RANGE",
    "SQLITE_READ",
    "SQLITE_READONLY_CANTINIT",
    "SQLITE_READONLY_CANTLOCK",
    "SQLITE_READONLY_DBMOVED",
    "SQLITE_READONLY_DIRECTORY",
    "SQLITE_READONLY_RECOVERY",
    "SQLITE_READONLY_ROLLBACK",
    "SQLITE_READONLY",
    "SQLITE_RECURSIVE",
    "SQLITE_REINDEX",
    "SQLITE_ROW",
    "SQLITE_SAVEPOINT",
    "SQLITE_SCHEMA",
    "SQLITE_SELECT",
    "SQLITE_TOOBIG",
    "SQLITE_TRANSACTION",
    "SQLITE_UPDATE",
    "sqlite_version_info",
    "sqlite_version",
    "SQLITE_WARNING_AUTOINDEX",
    "SQLITE_WARNING",
    "threadsafety",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "version",
    "Warning",
)
