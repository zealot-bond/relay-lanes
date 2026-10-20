#!/usr/bin/env bash
# Build the relay jar: compile the Java launcher, stage the payload, bundle a Node
# runtime, and zip it all up.
#
# The jar is the whole deliverable -- a Pterodactyl server only runs `java -jar
# server.jar` -- so everything the relay needs travels inside it.
#
#   NODE_RUNTIME=/path/to/node bash scripts/pack-jar.sh
#
# A build with no javac, no Node runtime, or no launcher source FAILS LOUDLY. A silent
# empty build once cost a whole debug cycle.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1

JAVAC="${JAVAC:-$(command -v javac 2>/dev/null)}"
if [ -z "$JAVAC" ] || [ ! -x "$JAVAC" ]; then
  echo "FAIL: no javac found. Install a JDK (17+) or set JAVAC=/path/to/javac" >&2
  exit 1
fi

NODE_RUNTIME="${NODE_RUNTIME:-}"
if [ -z "$NODE_RUNTIME" ] && [ -x /tmp/opencode/tool/noderuntime/bin/node ]; then
  NODE_RUNTIME=/tmp/opencode/tool/noderuntime
fi
if [ -z "$NODE_RUNTIME" ] && [ -d "$HOME/.tools" ]; then
  NODE_RUNTIME="$(ls -d "$HOME"/.tools/node* 2>/dev/null | head -1)"
fi
if [ -z "$NODE_RUNTIME" ] || [ ! -x "$NODE_RUNTIME/bin/node" ]; then
  CAND="$(command -v node 2>/dev/null)"
  if [ -n "$CAND" ]; then NODE_RUNTIME="$(dirname "$(dirname "$CAND")")"; fi
fi
if [ -z "$NODE_RUNTIME" ] || [ ! -x "$NODE_RUNTIME/bin/node" ]; then
  echo "FAIL: no node runtime found. Set NODE_RUNTIME=/path/to/node" >&2
  exit 1
fi

if [ ! -f launcher/com/relay/launcher/Launcher.java ]; then
  echo "FAIL: launcher/com/relay/launcher/Launcher.java is missing" >&2
  exit 1
fi

echo "    javac=$JAVAC"
echo "    node=$NODE_RUNTIME/bin/node"

rm -rf build/classes build/stage
mkdir -p build/classes build/stage/app-payload

echo "    compiling the launcher (Java 8 target)"
if ! "$JAVAC" --release 8 -d build/classes launcher/com/relay/launcher/Launcher.java 2>/tmp/javac.err; then
  echo "    (--release 8 unavailable; retrying with -source/-target)"
  "$JAVAC" -source 8 -target 8 -d build/classes launcher/com/relay/launcher/Launcher.java || {
    echo "FAIL: could not compile the launcher" >&2; cat /tmp/javac.err >&2; exit 1; }
fi

# stage the payload
cp -r src build/stage/app-payload/src
rm -rf build/stage/app-payload/src/test
if [ -f baked-credentials.json ]; then
  cp baked-credentials.json build/stage/app-payload/baked-credentials.json
  chmod 600 build/stage/app-payload/baked-credentials.json
  echo "    baked credentials included"
else
  echo "    WARNING: no baked-credentials.json -- the jar will have no credentials"
fi
printf '{"id":"local","built":"%s"}' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > build/stage/app-payload/build-info.json

echo "    bundling the node binary (only the binary: the rest of the install is npm and docs, and archiving those took 13 minutes)"
NODE_BIN="$NODE_RUNTIME/bin/node"
if [ ! -x "$NODE_BIN" ]; then echo "FAIL: $NODE_BIN is not executable" >&2; exit 1; fi
mkdir -p build/stage/app-payload/node/bin
cp "$NODE_BIN" build/stage/app-payload/node/bin/node
chmod +x build/stage/app-payload/node/bin/node
echo "    node binary $(du -h build/stage/app-payload/node/bin/node | cut -f1)"

# assemble the jar
STAGE=build/stage
mkdir -p "$STAGE/META-INF"
printf 'Manifest-Version: 1.0\nMain-Class: com.relay.launcher.Launcher\n\n' > "$STAGE/META-INF/MANIFEST.MF"
cp -r build/classes/. "$STAGE"/
if command -v jar >/dev/null 2>&1; then
  (cd "$STAGE" && jar cfm "$ROOT/server.jar" META-INF/MANIFEST.MF .) || { echo "FAIL: jar failed" >&2; exit 1; }
else
  (cd "$STAGE" && zip -q -r "$ROOT/server.jar" .) || { echo "FAIL: zip failed" >&2; exit 1; }
fi

echo "    build local built $(date -u +%Y-%m-%dT%H:%M:%SZ)"
ls -la server.jar
echo "built $(du -h server.jar | cut -f1)  ->  $ROOT/server.jar"