# Bundled Hadoop `winutils` (Windows only)

Spark 3.5 **cannot start on Windows** without Hadoop's `winutils.exe`: Hadoop's
`org.apache.hadoop.util.Shell` static initializer runs during
`SparkSubmit.prepareSubmitEnvironment` and throws
`HADOOP_HOME and hadoop.home.dir are unset` without it. These binaries are
bundled so Windows works out of the box, mirroring how the OneLake
token-provider jar is bundled.

- **Source**: https://github.com/cdarlint/winutils — `hadoop-3.3.5/bin`
- **Why 3.3.5**: Spark 3.5.0 bundles Hadoop 3.3.4 (`hadoop-client-api-3.3.4.jar`);
  3.3.4 is not published in that repo, and 3.3.5 is the nearest 3.3.x build.
- **Files / SHA256**:
  - `bin/winutils.exe` — `a0ca6e358357c41ef56ebdb02c38e4a4d55da7ca7a13001678bb2ef7d644adea`
  - `bin/hadoop.dll`   — `d3dd64afdc85f2a7eb5345abf2ecaa744b0a157de40859313337d47f81ee1c7b`

Both are PE32+ x86-64 Windows binaries. They are inert on Linux/macOS — the
resolver only consults them when `os.name == "nt"`.

To use your own Hadoop instead, set `runtime.hadoop_home` in `local-spark.toml`
(or `LOCAL_SPARK_HADOOP_HOME` / an ambient `HADOOP_HOME`); those take precedence
over the bundled copy.
