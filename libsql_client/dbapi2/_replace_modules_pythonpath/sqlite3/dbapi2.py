import os.path
import sys

# $LIBSQL_PYTHONPATH_BOOTSTRAP is set by libsql_client.dbapi2.__main__
bootstrap_path = os.environ["LIBSQL_PYTHONPATH_BOOTSTRAP"]

sqlite3_dbapi2_modname = __name__
sqlite3_modname = sqlite3_dbapi2_modname.split(".")[0]

# Remove itself from sys.path/$PYTHONPATH
#
# This MUST be done before libsql_client.dbapi2 is imported
# since it will use the stdlib sqlite3
sys.path.remove(bootstrap_path)
del sys.modules[sqlite3_modname]
del sys.modules[sqlite3_dbapi2_modname]

import libsql_client.dbapi2 as wrapper_dbapi2  # noqa: I900,E402

sys.modules[sqlite3_modname] = wrapper_dbapi2
sys.modules[sqlite3_dbapi2_modname] = wrapper_dbapi2
