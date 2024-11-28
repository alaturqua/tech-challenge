#!/bin/bash
set -e

case "$1" in
"pytest")
    exec python -m pytest "$2"
    ;;
"pyspark")
    if [ -n "$2" ]; then
        exec python "$2"
    else
        exec pyspark
    fi
    ;;
"mkdocs")
    exec mkdocs serve
    ;;
*)
    echo "Usage: $0 {pytest|pyspark|mkdocs} [path]"
    exit 1
    ;;
esac

# #!/bin/bash
# set -e
