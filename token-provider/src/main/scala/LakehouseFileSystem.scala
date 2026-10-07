package ch.fs

import java.net.URI
import java.nio.file.AccessDeniedException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

/** `lakehouse://<workspace-id>@<lakehouse-id>.onelake.dfs.fabric.microsoft.com/Files/x`
  *
  * A Hadoop filesystem whose root is one lakehouse, so that a Spark session whose
  * `fs.defaultFS` is this URI resolves a relative `Files/x` to that lakehouse's
  * OneLake `Files/`, as on Fabric. Every operation is translated to the inner
  * ABFS path `abfss://<workspace-id>@<host>/<lakehouse-id>/x` (authenticated by
  * `HttpTokenProvider` through the usual `fs.azure.*` configuration) and the
  * returned statuses are translated back, so Spark keeps reading through this
  * filesystem. Because the lakehouse id is in the authority, Hadoop caches one
  * instance per lakehouse and each Spark session (a local-spark context) can
  * point its own default filesystem at its own default lakehouse.
  *
  * Write policy: under `spark.localspark.write_mode` = sandbox or readonly (read
  * from the configuration this instance was created with), anything that would
  * modify OneLake (`create`, `append`, `mkdirs`, `delete`, `rename`, …) is
  * refused with an AccessDeniedException whose message says what to do instead;
  * writethrough passes everything through.
  *
  * `fs.lakehouse.inner` (default `abfss://<workspace-id>@<host>`) replaces the
  * inner root; tests point it at a local directory.
  */
class LakehouseFileSystem extends FileSystem {
  private var uri: URI = _
  private var workspaceId: String = _
  private var lakehouseId: String = _
  private var innerRoot: String = _  // ".../<lakehouse-id>" with no trailing slash
  private var inner: FileSystem = _
  private var writeMode: String = "sandbox"
  private var workingDir: Path = _

  override def getScheme: String = LakehouseFileSystem.Scheme

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    setConf(conf)
    workspaceId = Option(name.getUserInfo).getOrElse(
      throw new IllegalArgumentException(s"$name: expected lakehouse://<workspace-id>@<lakehouse-id>.<onelake host>"))
    val host = Option(name.getHost).getOrElse(throw new IllegalArgumentException(s"$name: no host"))
    val dot = host.indexOf('.')
    if (dot <= 0) throw new IllegalArgumentException(s"$name: host must be <lakehouse-id>.<onelake host>")
    lakehouseId = host.substring(0, dot)
    val onelakeHost = host.substring(dot + 1)
    uri = URI.create(s"${LakehouseFileSystem.Scheme}://$workspaceId@$host")
    val base = Option(conf.get("fs.lakehouse.inner")).map(_.stripSuffix("/")).getOrElse(s"abfss://$workspaceId@$onelakeHost")
    // Hadoop's canonical spelling (file:///x prints as file:/x), so prefix tests against returned paths hold
    innerRoot = new Path(s"$base/$lakehouseId").toString
    inner = FileSystem.get(new Path(innerRoot).toUri, conf)
    writeMode = Option(conf.get(OneLakeCatalog.WriteModeKey)).getOrElse("sandbox").trim.toLowerCase
    workingDir = new Path(uri.toString + "/")
  }

  override def getUri: URI = uri
  override def getWorkingDirectory: Path = workingDir
  override def setWorkingDirectory(dir: Path): Unit = { workingDir = makeQualified(dir) }
  override def getHomeDirectory: Path = workingDir

  override def makeQualified(path: Path): Path = path.makeQualified(uri, workingDir)

  private def isInnerPath(path: Path): Boolean = {
    val s = path.toString
    s == innerRoot || s.startsWith(innerRoot + "/")
  }

  override protected def checkPath(path: Path): Unit = {
    val s = path.toUri.getScheme
    if (s != null && s != LakehouseFileSystem.Scheme && !isInnerPath(path))
      throw new IllegalArgumentException(s"Wrong FS: $path, expected: $uri")
  }

  /** Our path -> the inner ABFS path (an inner path handed back to us is passed through). */
  private def toInner(path: Path): Path = {
    if (isInnerPath(path)) return path
    val q = makeQualified(path)
    val p = q.toUri.getPath
    new Path(if (p == null || p.isEmpty || p == "/") innerRoot else innerRoot + p)
  }

  /** An inner ABFS path -> ours (paths outside this lakehouse are left alone). */
  private def fromInner(path: Path): Path = {
    val s = path.toString
    if (s == innerRoot) new Path(uri.toString + "/")
    else if (s.startsWith(innerRoot + "/")) new Path(uri.toString + s.substring(innerRoot.length))
    else path
  }

  private def translate(st: FileStatus): FileStatus = {
    val copy = new FileStatus(st.getLen, st.isDirectory, st.getReplication, st.getBlockSize, st.getModificationTime,
      st.getAccessTime, st.getPermission, st.getOwner, st.getGroup,
      if (st.isSymlink) st.getSymlink else null, fromInner(st.getPath))
    copy
  }

  private def refuse(path: Path, op: String): Nothing =
    throw new AccessDeniedException(path.toString, null,
      s"write_mode=$writeMode: $op under the lakehouse's OneLake Files/ is refused because it would modify OneLake. " +
        "Write to /lakehouse/default/Files (the local mirror) instead, or start the runtime with write_mode = writethrough.")

  private def writable: Boolean = writeMode == "writethrough"

  // ---- reads ----
  override def open(f: Path, bufferSize: Int): FSDataInputStream = inner.open(toInner(f), bufferSize)
  override def getFileStatus(f: Path): FileStatus =
    try translate(inner.getFileStatus(toInner(f)))
    catch { case e: java.io.FileNotFoundException => throw new java.io.FileNotFoundException(s"$f: ${e.getMessage}") }
  override def listStatus(f: Path): Array[FileStatus] =
    try inner.listStatus(toInner(f)).map(translate)
    catch { case e: java.io.FileNotFoundException => throw new java.io.FileNotFoundException(s"$f: ${e.getMessage}") }
  override def getFileBlockLocations(file: FileStatus, start: Long, len: Long): Array[BlockLocation] =
    inner.getFileBlockLocations(inner.getFileStatus(toInner(file.getPath)), start, len)
  override def getContentSummary(f: Path): ContentSummary = inner.getContentSummary(toInner(f))

  // ---- writes (policy) ----
  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short,
                      blockSize: Long, progress: Progressable): FSDataOutputStream =
    if (writable) inner.create(toInner(f), permission, overwrite, bufferSize, replication, blockSize, progress)
    else refuse(f, "writing")
  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    if (writable) inner.append(toInner(f), bufferSize, progress) else refuse(f, "appending")
  override def rename(src: Path, dst: Path): Boolean =
    if (writable) inner.rename(toInner(src), toInner(dst)) else refuse(src, "renaming")
  override def delete(f: Path, recursive: Boolean): Boolean =
    if (writable) inner.delete(toInner(f), recursive) else refuse(f, "deleting")
  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    if (writable) inner.mkdirs(toInner(f), permission) else refuse(f, "creating a directory")
  override def setPermission(p: Path, permission: FsPermission): Unit =
    if (writable) inner.setPermission(toInner(p), permission) else refuse(p, "changing permissions")
  override def setOwner(p: Path, username: String, groupname: String): Unit =
    if (writable) inner.setOwner(toInner(p), username, groupname) else refuse(p, "changing the owner")
  override def setTimes(p: Path, mtime: Long, atime: Long): Unit =
    if (writable) inner.setTimes(toInner(p), mtime, atime) else refuse(p, "changing times")

  override def close(): Unit = { super.close() }  // the inner filesystem is cached by Hadoop; leave it open
}

object LakehouseFileSystem {
  val Scheme = "lakehouse"
  /** The URI a Spark session uses as `fs.defaultFS` so `Files/x` means this lakehouse's Files/x. */
  def uriFor(workspaceId: String, lakehouseId: String, onelakeHost: String = "onelake.dfs.fabric.microsoft.com"): String =
    s"$Scheme://$workspaceId@$lakehouseId.$onelakeHost"
}
