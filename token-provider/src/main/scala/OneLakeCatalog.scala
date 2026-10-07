package ch.fs

import java.util
import scala.collection.concurrent.TrieMap

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Column, Identifier, StagedTable, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.DeltaLog
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
 *   spark.localspark.dv_strategy         view (default) | clone — deletion-vector tables
 */
class OneLakeCatalog extends DeltaCatalog {
  import OneLakeCatalog._

  private def confOpt(key: String): Option[String] = spark.conf.getOption(key)
  private def writeMode: String = confOpt(WriteModeKey).getOrElse("sandbox").trim.toLowerCase
  private def host: String = confOpt(HostKey).getOrElse("onelake.dfs.fabric.microsoft.com")
  /** "view" (default; Delta 3.2) or "clone" (Delta >= 3.3) for deletion-vector tables. */
  private def dvStrategy: String = confOpt(DvStrategyKey).getOrElse("view").trim.toLowerCase
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

  /** A session database is either a lakehouse (`silver`) or a schema of a
    * schema-enabled lakehouse (`test__dbo`, Fabric's Tables/dbo/<table>). Returns
    * (lakehouse id, schema folder or ""). */
  private def lakehouseAndSchema(namespace: String): Option[(String, String)] =
    lakehouseId(namespace).map(id => (id, "")).orElse {
      val sep = namespace.lastIndexOf(SchemaSep)
      if (sep <= 0) None
      else lakehouseId(namespace.substring(0, sep)).map(id => (id, namespace.substring(sep + SchemaSep.length)))
    }

  private def oneLakePath(lakehouseId: String, table: String, schema: String = ""): String =
    if (schema.isEmpty) s"abfss://${workspaceId.get}@$host/$lakehouseId/Tables/$table"
    else s"abfss://${workspaceId.get}@$host/$lakehouseId/Tables/$schema/$table"

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

  /** Real (OneLake-cased) name of a table whose name matches case-insensitively.
    * Fabric's catalog is case-insensitive but OneLake paths are not, so
    * `dataverse.chtmotiftable` must reach `Tables/chtMotifTable`. One listing of
    * `Tables/` per lakehouse, cached briefly (new tables appear after the TTL). */
  private def realTableName(lakehouseId: String, schema: String, name: String): Option[String] = {
    val now = System.currentTimeMillis()
    val key = if (schema.isEmpty) lakehouseId else s"$lakehouseId/$schema"
    val listing = tableListCache.get(key) match {
      case Some((at, m)) if now - at < TableListTtlMs => m
      case _ =>
        try {
          val dir = if (schema.isEmpty) s"abfss://${workspaceId.get}@$host/$lakehouseId/Tables"
                    else s"abfss://${workspaceId.get}@$host/$lakehouseId/Tables/$schema"
          val tables = new Path(dir)
          val m = tables.getFileSystem(spark.sessionState.newHadoopConf()).listStatus(tables)
            .filter(_.isDirectory).map(st => st.getPath.getName.toLowerCase -> st.getPath.getName).toMap
          tableListCache.put(key, (now, m))  // only a successful listing is cached
          m
        } catch {
          case e: Exception =>
            System.err.println(s"local-spark: listing $key failed (${e.getClass.getSimpleName}: ${e.getMessage}); case-insensitive resolution unavailable this time")
            Map.empty[String, String]
        }
    }
    listing.get(name.toLowerCase)
  }

  /** (namespace, lakehouse id, OneLake path) when `ident` names an unregistered lakehouse table. */
  private def resolveOneLake(ident: Identifier): Option[(String, String, String)] = {
    if (Reentrant.get() || workspaceId.isEmpty) return None
    ident.namespace() match {
      case Array(ns) =>
        lakehouseAndSchema(ns).flatMap { case (id, schema) =>
          val exact = oneLakePath(id, ident.name(), schema)
          if (isDeltaDir(exact)) Some((ns, id, exact))
          else realTableName(id, schema, ident.name()) match {
            case Some(real) if real != ident.name() =>
              val src = oneLakePath(id, real, schema)
              if (isDeltaDir(src)) Some((ns, id, src)) else None
            case _ => None
          }
        }
      case _ => None
    }
  }

  private def quoted(s: String): String = "`" + s.replace("`", "``") + "`"
  /** Shadows are keyed by lakehouse id, not name, so projects that touch the same lakehouse share them. */
  private def shadowPath(lakehouseId: String, table: String): String = s"$shadowRoot/$lakehouseId/$table"
  private def shadowName(ns: String, table: String): String = {
    val sep = ns.lastIndexOf(SchemaSep)
    if (sep > 0 && lakehouseId(ns).isEmpty) s"${ns.substring(sep + SchemaSep.length)}.$table" else table
  }

  /** True when the source table's protocol declares the deletionVectors feature.
    * Delta 3.2 cannot SHALLOW CLONE such a table (the clone refuses the source's
    * deletion vectors, and forcing them trips the tightBounds check), so those
    * tables are materialized as live views instead — see materialize. */
  private def hasDeletionVectors(src: String): Boolean =
    try {
      val protocol = DeltaLog.forTable(spark, new Path(src)).update().protocol
      protocol.readerAndWriterFeatureNames.contains(DeletionVectorsFeature)
    } catch { case _: Exception => false }

  /** Register the table in the session catalog according to the write policy. */
  private def materialize(ident: Identifier, ns: String, id: String, src: String): Unit = {
    // Fully qualified on both sides: with a V2 catalog current (`USE <lakehouse>`
    // on a schema-enabled lakehouse) a two-part `delta.\`path\`` would resolve as
    // <catalog>.delta.<path> and fail with UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY,
    // and `<db>.<t>` would land in that catalog's namespace (Cobalt, 0.4.1).
    val name = s"spark_catalog.${quoted(ns)}.${quoted(ident.name())}"
    val t0 = System.nanoTime()
    var how = "external"
    Reentrant.set(true)
    try {
      if (writeMode == "writethrough") {
        spark.sql(s"CREATE TABLE IF NOT EXISTS $name USING DELTA LOCATION '$src'")
      } else {
        val shadow = shadowPath(id, shadowName(ns, ident.name()))
        if (isDeltaDir(shadow)) {
          how = "existing shadow"
          spark.sql(s"CREATE TABLE IF NOT EXISTS $name USING DELTA LOCATION '$shadow'")
        } else if (hasDeletionVectors(src)) {
          if (dvStrategy == "clone") {
            // Delta >= 3.3 can shallow-clone a deletion-vector table when the
            // clone enables the feature; the Python side picks this strategy.
            how = "shallow clone, deletion vectors"
            spark.sql(s"CREATE TABLE $name SHALLOW CLONE spark_catalog.delta.`$src` " +
              "TBLPROPERTIES ('delta.enableDeletionVectors'='true') " +
              s"LOCATION '$shadow'")
          } else {
            // Delta 3.2: live, read-only view. Reads go straight to OneLake; a
            // write is refused by Spark (a view) and explained by the Python
            // side, which finds these views by the comment tag.
            how = "read-only view, deletion vectors"
            spark.sql(s"CREATE VIEW IF NOT EXISTS $name COMMENT '$DvViewTag source=$src' AS SELECT * FROM spark_catalog.delta.`$src`")
          }
        } else {
          how = "shallow clone"
          spark.sql(s"CREATE TABLE $name SHALLOW CLONE spark_catalog.delta.`$src` LOCATION '$shadow'")
        }
      }
      // The Python side drains this after each cell and reports "mounted X in N s",
      // so first-touch cost never hides inside a notebook's own timer (ADO #298).
      Materialized.add(s"$ns.${ident.name()}\t${(System.nanoTime() - t0) / 1e6}\t$how")
    } finally Reentrant.set(false)
  }

  override def loadTable(ident: Identifier): Table =
    try super.loadTable(ident)
    catch {
      case e: NoSuchTableException =>
        resolveOneLake(ident) match {
          case Some((ns, id, src)) =>
            // One clone per table, however many threads touch it at once: the
            // first materializes, the rest wait and reuse (ADO #301: concurrent
            // first touch raced on the clone commit, DELTA_CONCURRENT_WRITE or a
            // corrupt 00000000000000000000.json).
            val key = s"${ns.toLowerCase}.${ident.name().toLowerCase}"
            val lock = materializeLocks.getOrElseUpdate(key, new Object)
            lock.synchronized {
              if (!super.tableExists(ident)) materialize(ident, ns, id, src)
            }
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
        lakehouseAndSchema(ns).map(_._1) match {
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

  /** Before a REPLACE of a Delta table, reset its session-catalog entry to the
    * shape Delta registers for a fresh table: empty schema, no partition columns.
    * Delta's post-commit UpdateCatalog hook stores the new data schema through
    * ExternalCatalog.alterTableDataSchema, and Spark's InMemoryCatalog asserts
    * that the entry's partition columns are the trailing schema columns; after
    * one replace of a partitioned table they are not, so the NEXT replace commits
    * and then throws "[INTERNAL_ERROR] Eagerly executed replace failed / Corrupted
    * table metadata" (ADO #300). Hive-backed catalogs (Fabric) never assert. The
    * entry's schema is cosmetic for Delta tables: Delta reads it from the log. */
  private def resetCatalogEntryForReplace(ident: Identifier): Unit = {
    if (Reentrant.get()) return
    try {
      val cat = spark.sessionState.catalog
      val ti = TableIdentifier(ident.name(), ident.namespace().headOption)
      if (cat.tableExists(ti)) {
        val t = cat.getTableMetadata(ti)
        val isDelta = t.provider.exists(_.equalsIgnoreCase("delta"))
        if (isDelta && (t.partitionColumnNames.nonEmpty || t.schema.nonEmpty))
          cat.alterTable(t.copy(schema = new StructType(), partitionColumnNames = Seq.empty))
      }
    } catch { case _: Exception => () }  // best effort; the replace proceeds either way
  }

  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
                            props: util.Map[String, String]): StagedTable = {
    resetCatalogEntryForReplace(ident)
    super.stageReplace(ident, schema, partitions, policed(ident, props))
  }

  override def stageReplace(ident: Identifier, columns: Array[Column], partitions: Array[Transform],
                            props: util.Map[String, String]): StagedTable = {
    resetCatalogEntryForReplace(ident)
    super.stageReplace(ident, columns, partitions, policed(ident, props))
  }

  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform],
                                    props: util.Map[String, String]): StagedTable = {
    resetCatalogEntryForReplace(ident)
    super.stageCreateOrReplace(ident, schema, partitions, policed(ident, props))
  }

  override def stageCreateOrReplace(ident: Identifier, columns: Array[Column], partitions: Array[Transform],
                                    props: util.Map[String, String]): StagedTable = {
    resetCatalogEntryForReplace(ident)
    super.stageCreateOrReplace(ident, columns, partitions, policed(ident, props))
  }
}

object OneLakeCatalog {
  /** Session-database separator for a schema of a schema-enabled lakehouse: `test__dbo`. */
  val SchemaSep = "__"

  /** Table names under a lakehouse's Tables/ as seen on OneLake: `table`, or
    * `schema/table` for a schema-enabled lakehouse (a non-Delta directory whose
    * children are Delta tables). Uses the active session's authenticated
    * filesystem, so no Fabric REST call and no extra credential. */
  def listOneLakeTables(workspaceId: String, lakehouseId: String): java.util.List[String] = {
    val spark = org.apache.spark.sql.SparkSession.active
    val host = spark.conf.getOption(HostKey).getOrElse("onelake.dfs.fabric.microsoft.com")
    val root = new Path(s"abfss://$workspaceId@$host/$lakehouseId/Tables")
    val fs = root.getFileSystem(spark.sessionState.newHadoopConf())
    val out = new java.util.ArrayList[String]()
    if (!fs.exists(root)) return out
    def isDelta(p: Path): Boolean = try fs.exists(new Path(p, "_delta_log")) catch { case _: Exception => false }
    for (st <- fs.listStatus(root) if st.isDirectory) {
      val name = st.getPath.getName
      if (isDelta(st.getPath)) out.add(name)
      else if (!name.startsWith("_") && !name.startsWith(".")) {
        for (child <- fs.listStatus(st.getPath) if child.isDirectory && isDelta(child.getPath))
          out.add(s"$name/${child.getPath.getName}")
      }
    }
    out
  }

  /** Materializations since the last drain: "ns.table<TAB>millis<TAB>how". */
  private val Materialized = new java.util.concurrent.ConcurrentLinkedQueue[String]()
  def drainMaterialized(): java.util.List[String] = {
    val out = new java.util.ArrayList[String]()
    var item = Materialized.poll()
    while (item != null) { out.add(item); item = Materialized.poll() }
    out
  }
  val DeletionVectorsFeature = "deletionVectors"
  val DvStrategyKey = "spark.localspark.dv_strategy"
  /** Comment prefix on the view that stands in for a deletion-vector table. */
  val DvViewTag = "localspark:deletion-vectors"
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
  /** lowercased "ns.table" -> monitor serializing its first-touch materialization */
  private val materializeLocks = TrieMap.empty[String, Object]
  /** lakehouse id -> (listed at, lowercased name -> OneLake-cased name) */
  private val tableListCache = TrieMap.empty[String, (Long, Map[String, String])]
  val TableListTtlMs = 60000L
}
