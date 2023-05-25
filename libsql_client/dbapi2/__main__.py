import os
import os.path
import sys

args = sys.argv[1:]
env = os.environ

bootstrap_path = os.path.join(
    os.path.dirname(__file__),
    "_replace_modules_pythonpath",
)

if not args or args[0].lower() in ("-h", "--help", "-?"):
    sys.stderr.write(
        "Usage:\n"
        "\t<program> [program-args...]\n"
        "\n"
        "This will execute the python program transparently "
        "replacing 'sqlite3' and 'sqlite3.dbapi2' imports with "
        "'libsql_client.dbapi2' by adding "
        f"'{bootstrap_path}' to $PYTHONPATH"
        "\n"
    )
    sys.exit(0)


py_path = env.get("PYTHONPATH", "")
if not py_path:
    py_path = bootstrap_path
else:
    py_path = os.pathsep.join([bootstrap_path, py_path])

env["LIBSQL_PYTHONPATH_BOOTSTRAP"] = bootstrap_path
env["PYTHONPATH"] = py_path

os.execvpe(args[0], args, env)
