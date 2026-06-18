package ch.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee

import java.net.{HttpURLConnection, URL}
import java.time.Instant
import java.util.{Base64, Date}
import scala.io.Source
import scala.util.matching.Regex

/**
 * Hadoop ABFS custom token provider that fetches OneLake bearer tokens from a
 * loopback HTTP endpoint served by the local-spark-mcp server (which mints them
 * via Azure DefaultAzureCredential). The endpoint URL and a shared secret are
 * passed in via Hadoop configuration:
 *
 *   fs.azure.tokenprovider.endpoint   (required) e.g. http://127.0.0.1:54321/token
 *   fs.azure.tokenprovider.secret     (required) sent as the X-Token-Secret header
 *
 * Set these through Spark as spark.hadoop.fs.azure.tokenprovider.* .
 */
class HttpTokenProvider extends CustomTokenProviderAdaptee {

  private val EndpointKey = "fs.azure.tokenprovider.endpoint"
  private val SecretKey = "fs.azure.tokenprovider.secret"
  private val SecretHeader = "X-Token-Secret"
  private val RefreshSkewMillis = 5 * 60 * 1000L // refresh 5 min before expiry

  private var endpoint: String = _
  private var secret: String = ""
  private var cachedToken: Option[String] = None
  private var expiryTime: Date = new Date(0L)

  override def initialize(conf: Configuration, accountName: String): Unit = {
    endpoint = conf.get(EndpointKey)
    secret = Option(conf.get(SecretKey)).getOrElse("")
    if (endpoint == null || endpoint.isEmpty) {
      throw new IllegalArgumentException(s"$EndpointKey is not configured")
    }
  }

  override def getAccessToken(): String = {
    val stillFresh = cachedToken.isDefined &&
      expiryTime.getTime - System.currentTimeMillis() > RefreshSkewMillis
    if (stillFresh) return cachedToken.get

    val token = fetchToken()
    expiryTime = parseExpiry(token)
    cachedToken = Some(token)
    token
  }

  override def getExpiryTime: Date = expiryTime

  private def fetchToken(): String = {
    val conn = new URL(endpoint).openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod("GET")
      if (secret.nonEmpty) conn.setRequestProperty(SecretHeader, secret)
      conn.setConnectTimeout(5000)
      conn.setReadTimeout(15000)
      val code = conn.getResponseCode
      if (code != 200) {
        throw new RuntimeException(s"token endpoint $endpoint returned HTTP $code")
      }
      val src = Source.fromInputStream(conn.getInputStream, "UTF-8")
      try src.mkString.trim
      finally src.close()
    } finally {
      conn.disconnect()
    }
  }

  /** Read the JWT "exp" claim; fall back to a short TTL if it can't be parsed. */
  private def parseExpiry(token: String): Date = {
    val fallback = Date.from(Instant.now().plusSeconds(300))
    val parts = token.split("\\.")
    if (parts.length < 2) return fallback
    try {
      val payload = new String(Base64.getUrlDecoder.decode(pad(parts(1))), "UTF-8")
      val expPattern = new Regex("\"exp\"\\s*:\\s*(\\d+)")
      expPattern.findFirstMatchIn(payload) match {
        case Some(m) => Date.from(Instant.ofEpochSecond(m.group(1).toLong))
        case None    => fallback
      }
    } catch {
      case _: Throwable => fallback
    }
  }

  private def pad(s: String): String = s.length % 4 match {
    case 0 => s
    case r => s + ("=" * (4 - r))
  }
}
