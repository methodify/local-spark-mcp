package ch.fs

import java.util
import scala.collection.concurrent.TrieMap

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Column, Identifier, StagedTable, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.types.StructType

/**
 * The session catalog for local-spark-mcp: DeltaCatalog plus on-demand
 * resolution of Fabric lakehouse tables, so `lakehouse.table` (and an
 * unqualified `table` under the current database) works the way it does on the
 * Fabric runtime — for every code path, with no mount step and no monkey-patch.
 *
 * On a catalog miss for `<lakehouse>.<table>` where `<lakehouse>` is registered
 * and `Tables/<table>/_delta_log` exists in OneLake, the table is materialized
 * into the session catalog on first touch, which also makes V1-only APIs such
 * as `DeltaTable.forName` work afterwards. Materialization honors the write
 * policy:
 *
 *   - sandbox / readonly: a Delta SHALLOW CLONE under the shadow root. Metadata
 *     only; data files are read from OneLake in place, and any write lands in
 *     the local clone. OneLake is never modified.
 *   - writethrough: an external table pointing straight at the OneLake path.
 *
 * `createTable` (new tables via saveAsTable / CREATE TABLE) applies the same
 * policy: readonly refuses, sandbox lands in the shadow root, writethrough lands
 * under the lakehouse's `Tables/`.
 *
 * Configuration is read from Spark confs so the Python side can set it at
 * session build (and register lakehouses at runtime):
 *
 *   spark.localspark.workspace_id        Fabric workspace GUID
 *   spark.localspark.lakehouse.<name>    lakehouse GUID, one key per lakehouse
 *   spark.localspark.write_mode          sandbox | readonly | writethrough
 *   spark.localspark.shadow_root         local dir for clones and new tables
 *   spark.localspark.onelake_host        default onelake.dfs.fabric.microsoft.com
 */
class OneLakeCatalog extends DeltaCatalog {
  import OneLakeCatalog._

  private def confOpt(key: String): Option[String] = spark.conf.getOption(key)
  private def writeMode: String = confOpt(WriteModeKey).getOrElse("sandbox").trim.toLowerCase
  private def host: String = confOpt(HostKey).getOrElse("onelake.dfs.fabric.microsoft.com")
  private def workspaceId: Option[String] = confOpt(WorkspaceKey).filter(_.nonEmpty)
  private def shadowRoot: String = confOpt(ShadowRootKey).filter(_.nonEmpty).getOrElse(
    throw new IllegalStateException(s"$ShadowRootKey is not set")
  )

  /** Case-insensitive lookup: Spark normalizes catalog identifiers to lower case. */
  private def lakehouseId(namespace: String): Option[String] =
    spark.conf.getAll.collectFirst {
      case (k, v) if k.startsWith(LakehousePrefix) &&
        k.substring(LakehousePrefix.length).equalsIgnoreCase(namespace) => v
    }

  private def oneLakePath(lakehouseId: String, table: String): String =
    s"abfss://${workspaceId.get}@$host/$lakehouseId/Tables/$table"

  private def isDeltaDir(path: String): Boolean = {
    // Only OneLake paths are cached: each check is a network round trip, and
    // OneLake tables don't vanish under us. Local shadow paths are re-checked so
    // discard_shadow can never leave a stale positive behind.
    val cacheable = path.startsWith("abfss://")
    if (cacheable && existsCache.contains(path)) return true
    val log = new Path(path, "_delta_log")
    val found =
      try log.getFileSystem(spark.sessionState.newHadoopConf()).exists(log)
      catch { case _: Exception => false }
    if (found && cacheable) existsCache.put(path, true)
    found
  }

  /** (namespace, lakehouse id, OneLake path) when `ident` names an unregistered lakehouse table. */
  private def resolveOneLake(ident: Identifier): Option[(String, String, String)] = {
    if (Reentrant.get() || workspaceId.isEmpty) return None
    ident.namespace() match {
      case Array(ns) =>
        lakehouseId(ns).flatMap { id =>
          val src = oneLakePath(id, ident.name())
          if (isDeltaDir(src)) Some((ns, id, src)) else None
        }
      case _ => None
    }
  }

  private def quoted(s: String): String = "`" + s.replace("`", "``") + "`"
  /** Shadows are keyed by lakehouse id, not name, so projects that touch the same lakehouse share them. */
  private def shadowPath(lakehouseId: String, table: String): String = s"$shadowRoot/$lakehouseId/$table"

  /** Register the table in the session catalog according to the write policy. */
  private def materialize(ident: Identifier, ns: String, id: String, src: String): Unit = {
    val name = s"${quoted(ns)}.${quoted(ident.name())}"
    Reentrant.set(true)
    try {
      if (writeMode == "writethrough") {
        spark.sql(s"CREATE TABLE IF NOT EXISTS $name USING DELTA LOCATION '$src'")
      } else {
        val shadow = shadowPath(id, ident.name())
        if (isDeltaDir(shadow)) {
          spark.sql(s"CREATE TABLE IF NOT EXISTS $name USING DELTA LOCATION '$shadow'")
        } else {
          spark.sql(s"CREATE TABLE $name SHALLOW CLONE delta.`$src` LOCATION '$shadow'")
        }
      }
    } finally Reentrant.set(false)
  }

  override def loadTable(ident: Identifier): Table =
    try super.loadTable(ident)
    catch {
      case e: NoSuchTableException =>
        resolveOneLake(ident) match {
          case Some((ns, id, src)) =>
            materialize(ident, ns, id, src)
            super.loadTable(ident)
          case None => throw e
        }
    }

  override def tableExists(ident: Identifier): Boolean =
    super.tableExists(ident) || resolveOneLake(ident).isDefined

  /** Apply the write policy to a new table under a lakehouse namespace. */
  private def policed(ident: Identifier, props: util.Map[String, String]): util.Map[String, String] = {
    if (Reentrant.get()) return props
    ident.namespace() match {
      case Array(ns) =>
        lakehouseId(ns) match {
          case None => props
          case Some(id) =>
            val hasLocation = props.containsKey("location")
            writeMode match {
              case "readonly" =>
                throw new UnsupportedOperationException(
                  s"write_mode is 'readonly': refusing to create table $ns.${ident.name()}. " +
                  "Set LOCAL_SPARK_WRITE_MODE (or [runtime] write_mode in local-spark.toml) " +
                  "to 'sandbox' to write locally, or 'writethrough' to write to OneLake.")
              case "writethrough" if !hasLocation =>
                withLocation(props, oneLakePath(id, ident.name()))
              case "sandbox" if !hasLocation =>
                withLocation(props, shadowPath(id, ident.name()))
              case _ => props
            }
        }
      case _ => props
    }
  }

  private def withLocation(props: util.Map[String, String], location: String): util.Map[String, String] = {
    val m = new util.HashMap[String, String](props)
    m.put("location", location)
    m
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform],
                           props: util.Map[String, String]): Table =
    super.createTable(ident, schema, partitions, policed(ident, props))

  override def createTable(ident: Identifier, columns: Array[Column], partitions: Array[Transform],
                           props: util.Map[String, String]): Table =
    super.createTable(ident, columns, partitions, policed(ident, props))

  // DeltaCatalog is a StagingTableCatalog, so CTAS / RTAS — which is what
  // DataFrame.saveAsTable is — go through these, not createTable.
  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform],
                           props: util.Map[String, String]): StagedTable =
    super.stageCreate(ident, schema, partitions, policed(ident, props))

  override def stageCreate(ident: Identifier, columns: Array[Column], partitions: Array[Transform],
                           props: util.Map[String, String]): StagedTable =
    super.stageCreate(ident, columns, partitions, policed(ident, props))

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
                            props: util.Map[String, String]): StagedTable =
    super.stageReplace(ident, schema, partitions, policed(ident, props))

  override def stageReplace(ident: Identifier, columns: Array[Column], partitions: Array[Transform],
                            props: util.Map[String, String]): StagedTable =
    super.stageReplace(ident, columns, partitions, policed(ident, props))

  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
                                    props: util.Map[String, String]): StagedTable =
    super.stageCreateOrReplace(ident, schema, partitions, policed(ident, props))

  override def stageCreateOrReplace(ident: Identifier, columns: Array[Column], partitions: Array[Transform],
                                    props: util.Map[String, String]): StagedTable =
    super.stageCreateOrReplace(ident, columns, partitions, policed(ident, props))
}

object OneLakeCatalog {
  val WorkspaceKey = "spark.localspark.workspace_id"
  val LakehousePrefix = "spark.localspark.lakehouse."
  val WriteModeKey = "spark.localspark.write_mode"
  val ShadowRootKey = "spark.localspark.shadow_root"
  val HostKey = "spark.localspark.onelake_host"

  /** Set while materializing so the nested CREATE TABLE doesn't re-enter the resolver. */
  private val Reentrant: ThreadLocal[Boolean] = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }
  /** Positive-only cache of `_delta_log` existence checks (each is a OneLake round trip). */
  private val existsCache = TrieMap.empty[String, Boolean]
}
