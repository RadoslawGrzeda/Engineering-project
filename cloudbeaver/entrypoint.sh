#!/bin/sh
# Seed data-sources.json only on first run (never overwrite user-made changes)
TARGET=/opt/cloudbeaver/workspace/GlobalConfiguration/.dbeaver/data-sources.json
if [ ! -f "$TARGET" ]; then
    mkdir -p "$(dirname "$TARGET")"
    envsubst < /init/data-sources.json.tpl > "$TARGET"
fi
exec /opt/cloudbeaver/launch-product.sh 
