#!/usr/bin/env bash
# Build the catalog/token-provider jar for BOTH runtime profiles (Scala 2.12 for
# fabric-1.3, Scala 2.13 for fabric-2.0) and copy them into the Python package so
# they ship in the wheel (uvx/pip installs from GitHub need them for Fabric
# mode). Re-run and commit the result whenever anything under
# token-provider/src changes.
#
# Requires Java 17 + sbt. Set JAVA_HOME to a JDK 17 (e.g. a vfox-managed one):
#   JAVA_HOME=~/.version-fox/cache/java/v-17.0.16-bsg/java-17.0.16-bsg scripts/build_jar.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here/token-provider"
sbt -batch +package

dest="$here/src/local_spark_mcp/jars"
mkdir -p "$dest"
rm -f "$dest"/*.jar
cp target/scala-2.12/*.jar target/scala-2.13/*.jar "$dest"/
echo "Bundled:"; ls -la "$dest"/*.jar
