package ch.fs

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * `lakehouse.schema.table` for a schema-enabled lakehouse, spelled as on Fabric.
 *
 * Spark's session catalog has single-level namespaces, so each schema of such a
 * lakehouse is a session database `<lakehouse>__<schema>` (OneLakeCatalog resolves
 * it to Tables/<schema>/<table> with the write policy). This catalog, registered
 * as `spark.sql.catalog.<lakehouse>`, is a thin translator onto the session
 * catalog: `test.dbo.x` -> `spark_catalog.test__dbo.x`, `test.x` -> `spark_catalog.test.x`,
 * `USE test` -> default schema (dbo). Every table and staging operation delegates,
 * so clones, the write policy, notices and shadows all behave as elsewhere.
 *
 * Options: lakehouse (name), default_schema (default dbo).
 */
class OneLakeSchemaCatalog extends TableCatalog with StagingTableCatalog with SupportsNamespaces {
  private var catalogName: String = _
  private var lakehouse: String = _
  private var defaultSchema: String = "dbo"

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    lakehouse = Option(options.get("lakehouse")).getOrElse(name)
    defaultSchema = Option(options.get("default_schema")).getOrElse("dbo")
  }

  override def name(): String = catalogName
  override def defaultNamespace(): Array[String] = Array(defaultSchema)

  private def spark: SparkSession = SparkSession.active
  private def session: TableCatalog with StagingTableCatalog with SupportsNamespaces =
    spark.sessionState.catalogManager.catalog("spark_catalog").asInstanceOf[TableCatalog with StagingTableCatalog with SupportsNamespaces]

  /** [dbo] -> test__dbo ; [] -> test. A one-part namespace that is not one of this
    * lakehouse's schemas but is a session database (another lakehouse, or any
    * database) passes through unchanged, so `customer.sources_name` keeps working
    * while this catalog is current (Fabric resolves `x.y` against the current
    * lakehouse first; here the fallback keeps two-part lakehouse names alive). */
  private def db(ns: Array[String]): String = ns match {
    case Array() => lakehouse
    case Array(schema) =>
      val schemaDb = s"$lakehouse${OneLakeCatalog.SchemaSep}$schema"
      if (session.namespaceExists(Array(schemaDb))) schemaDb
      else if (session.namespaceExists(Array(schema))) schema
      else schemaDb
    case other => throw new NoSuchNamespaceException(other)
  }
  /** delta.`/abs/path` (and any `<source>.`<absolute path>``): a path table the
    * session catalog knows how to load; hand it over untranslated so the form keeps
    * working while this catalog is current. */
  private def isPathIdent(ident: Identifier): Boolean =
    ident.namespace().length == 1 && ident.name().contains("/") &&
      scala.util.Try(new org.apache.hadoop.fs.Path(ident.name()).isAbsolute).getOrElse(false)
  private def translate(ident: Identifier): Identifier =
    if (isPathIdent(ident)) ident else Identifier.of(Array(db(ident.namespace())), ident.name())
  private def back(ident: Identifier): Identifier = {
    val d = ident.namespace().headOption.getOrElse("")
    val prefix = lakehouse + OneLakeCatalog.SchemaSep
    if (d.startsWith(prefix)) Identifier.of(Array(d.substring(prefix.length)), ident.name())
    else Identifier.of(Array.empty[String], ident.name())
  }

  // ---- tables ----
  override def listTables(namespace: Array[String]): Array[Identifier] =
    session.listTables(Array(db(namespace))).map(back)
  override def loadTable(ident: Identifier): Table =
    try session.loadTable(translate(ident))
    catch { case _: NoSuchTableException => throw new NoSuchTableException(ident) }
  override def tableExists(ident: Identifier): Boolean = session.tableExists(translate(ident))
  override def invalidateTable(ident: Identifier): Unit = session.invalidateTable(translate(ident))
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], props: util.Map[String, String]): Table =
    session.createTable(translate(ident), schema, partitions, props)
  override def createTable(ident: Identifier, columns: Array[Column], partitions: Array[Transform], props: util.Map[String, String]): Table =
    session.createTable(translate(ident), columns, partitions, props)
  override def alterTable(ident: Identifier, changes: TableChange*): Table = session.alterTable(translate(ident), changes: _*)
  override def dropTable(ident: Identifier): Boolean = session.dropTable(translate(ident))
  override def purgeTable(ident: Identifier): Boolean = session.purgeTable(translate(ident))
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = session.renameTable(translate(oldIdent), translate(newIdent))

  // ---- staging (saveAsTable / CTAS / REPLACE go through here) ----
  override def stageCreate(ident: Identifier, schema: StructType, partitions: Array[Transform], props: util.Map[String, String]): StagedTable =
    session.stageCreate(translate(ident), schema, partitions, props)
  override def stageCreate(ident: Identifier, columns: Array[Column], partitions: Array[Transform], props: util.Map[String, String]): StagedTable =
    session.stageCreate(translate(ident), columns, partitions, props)
  override def stageReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], props: util.Map[String, String]): StagedTable =
    session.stageReplace(translate(ident), schema, partitions, props)
  override def stageReplace(ident: Identifier, columns: Array[Column], partitions: Array[Transform], props: util.Map[String, String]): StagedTable =
    session.stageReplace(translate(ident), columns, partitions, props)
  override def stageCreateOrReplace(ident: Identifier, schema: StructType, partitions: Array[Transform], props: util.Map[String, String]): StagedTable =
    session.stageCreateOrReplace(translate(ident), schema, partitions, props)
  override def stageCreateOrReplace(ident: Identifier, columns: Array[Column], partitions: Array[Transform], props: util.Map[String, String]): StagedTable =
    session.stageCreateOrReplace(translate(ident), columns, partitions, props)

  // ---- namespaces = schemas ----
  override def listNamespaces(): Array[Array[String]] = {
    val prefix = lakehouse + OneLakeCatalog.SchemaSep
    session.listNamespaces().collect { case Array(d) if d.startsWith(prefix) => Array(d.substring(prefix.length)) }
  }
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] =
    if (namespace.isEmpty) listNamespaces() else Array.empty
  override def namespaceExists(namespace: Array[String]): Boolean =
    namespace.isEmpty || session.namespaceExists(Array(db(namespace)))
  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    if (!namespaceExists(namespace)) throw new NoSuchNamespaceException(namespace)
    new util.HashMap[String, String]()
  }
  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit =
    session.createNamespace(Array(db(namespace)), metadata)
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    session.alterNamespace(Array(db(namespace)), changes: _*)
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    session.dropNamespace(Array(db(namespace)), cascade)
}
