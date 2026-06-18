package ch.fs

import org.apache.hadoop.conf.Configuration

/**
 * Manual JVM-side check of HttpTokenProvider WITHOUT Azure: point it at a local
 * token endpoint (e.g. the Python test server returning a crafted JWT) and print
 * the token + parsed expiry. Run with:
 *
 *   sbt "runMain ch.fs.TokenProbe <endpoint-url> <secret>"
 */
object TokenProbe {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("usage: TokenProbe <endpoint-url> [secret]")
      sys.exit(2)
    }
    val conf = new Configuration()
    conf.set("fs.azure.tokenprovider.endpoint", args(0))
    if (args.length > 1) conf.set("fs.azure.tokenprovider.secret", args(1))

    val provider = new HttpTokenProvider()
    provider.initialize(conf, "onelake.dfs.fabric.microsoft.com")
    val token = provider.getAccessToken()
    println(s"TOKEN=$token")
    println(s"EXPIRY=${provider.getExpiryTime}")
    // Second call should hit the in-memory cache (no fetch).
    val again = provider.getAccessToken()
    println(s"CACHED_MATCH=${again == token}")
  }
}
