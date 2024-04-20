// This is example of Jedis Application Client Library to connect to
// Memorystore Redis Cluster with Authentication and TLS Enabled

package com.example;

import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import java.io.*;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.DefaultRedisCredentials;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.RedisCredentials;
import redis.clients.jedis.RedisCredentialsProvider;

public class App {
  /**
   * This thread-safe implementation (excluding the main app below) is intended for production use.
   * It provides a background refresh logic that shouldn't overload IAM service in case. the
   * application has many many connections (connection storms can result in IAM throttles
   * otherwise). <br>
   * <br>
   * Guidelines for implementing similar logic for other clients:<br>
   * 1. Refresh IAM tokens in the background using a single thread/routine per client process<br>
   * 2. Provide last error feedback inline for token retrieval to aid debugging<br>
   * 3. Provide initial setup validation by fast-failing if the token couldn't be retrieved<br>
   * 4. Inline getToken shouldn't execute direct IAM calls as it can overload the token retrieval
   * resulting in throttles<br>
   * 5. Typical scale is tens of thousands of Redis connections and the IAM token is required for
   * every connection being established.<br>
   */
  private static final class RedisClusterCredentialsProvider
      implements RedisCredentialsProvider, Runnable, Closeable {
    private static final Logger logger =
        Logger.getLogger(RedisClusterCredentialsProvider.class.getName());

    private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

    private IamCredentialsClient iamClient;
    private String accountName;
    private int refreshDurationSec;
    private int lifetimeSec;

    private volatile RedisCredentials credentials;
    private volatile Instant lastRefreshInstant;
    private volatile RuntimeException lastException;

    // AccountName:
    // "projects/-/serviceAccounts/example-service-account@example-project.iam.gserviceaccount.com";
    // RefreshDuration: Duration.ofSeconds(300);
    // Lifetime: Duration.ofSeconds(3600);
    public RedisClusterCredentialsProvider(String accountName, int refreshDuration, int lifetime) {
      this.accountName = accountName;
      this.refreshDurationSec = refreshDuration;
      this.lifetimeSec = lifetime;

      try {
        this.iamClient = IamCredentialsClient.create();
      } catch (Exception ex) {
        // exception handling
        ex.printStackTrace();
        this.iamClient = null;
        return;
      }
      // execute on initialization to fast-fail if there are any setup issues
      refreshTokenNow();
      // refresh much more frequently than the expiry time to allow for multiple retries in case of
      // failures
      service.scheduleWithFixedDelay(this, 10, 10, TimeUnit.SECONDS);
    }

    @Override
    public RedisCredentials get() {
      if (hasTokenExpired()) {
        throw new RuntimeException("Background IAM token refresh failed", lastException);
      }
      return this.credentials;
    }

    private boolean hasTokenExpired() {
      if (this.lastRefreshInstant == null || this.lifetimeSec <= 0) {
        return true;
      }
      return Instant.now()
          .isAfter(this.lastRefreshInstant.plus(Duration.ofSeconds(this.lifetimeSec)));
    }

    // To be invoked by customer app on shutdown
    @Override
    public void close() {
      service.shutdown();
      iamClient.close();
    }

    @Override
    public void run() {
      try {
        // fetch token if it is time to refresh
        if (this.lastRefreshInstant != null
            && this.refreshDurationSec > 0
            && Instant.now()
                .isBefore(
                    this.lastRefreshInstant.plus(Duration.ofSeconds(this.refreshDurationSec)))) {
          // nothing to do
          return;
        }
        refreshTokenNow();
      } catch (Exception e) {
        // suppress all errors as we cannot allow the task to die
        // log for visibility
        logger.log(Level.parse("SEVERE"), "Background IAM token refresh failed", e);
      }
    }

    private void refreshTokenNow() {
      try {
        logger.info("Refreshing IAM token");
        List<String> delegates = new ArrayList<>();
        GenerateAccessTokenResponse response =
            this.iamClient.generateAccessToken(
                this.accountName,
                delegates,
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"),
                com.google.protobuf.Duration.newBuilder().setSeconds(this.lifetimeSec).build());

        // got a successful token refresh
        this.credentials = new DefaultRedisCredentials("default", response.getAccessToken());
        this.lastRefreshInstant = Instant.now();
        // clear the last saved exception
        this.lastException = null;
        logger.info(
            "IAM token refreshed with lastRefreshInstant ["
                + lastRefreshInstant
                + "], refreshDuration ["
                + this.refreshDurationSec
                + " seconds], accountName ["
                + this.accountName
                + "] and lifetime ["
                + this.lifetimeSec
                + " seconds]");
      } catch (Exception e) {
        // Bubble up for direct feedback
        throw e;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Hello World!");

    GenericObjectPoolConfig<Connection> config = new GenericObjectPoolConfig<Connection>();
    config.setTestWhileIdle(true);
    int timeout = 5000;
    int maxAttemps = 5;
    HostAndPort discovery = new HostAndPort("10.142.0.12", 6379);

    RedisCredentialsProvider credentialsProvider =
        new RedisClusterCredentialsProvider(
            "projects/-/serviceAccounts/aoguo-test-sa@aoguo-test.iam.gserviceaccount.com",
            300,
            3600);

    // Create JedisCluster instance
    InputStream is = new FileInputStream("server-ca.pem");
    // You could get a resource as a stream instead.

    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    X509Certificate caCert = (X509Certificate) cf.generateCertificate(is);

    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null); // You don't need the KeyStore instance to come from a file.
    ks.setCertificateEntry("caCert", caCert);

    tmf.init(ks);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), null);
    JedisCluster jedisCluster =
        new JedisCluster(
            discovery,
            DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(timeout)
                .socketTimeoutMillis(timeout)
                .credentialsProvider(credentialsProvider)
                .ssl(true)
                .sslSocketFactory(sslContext.getSocketFactory())
                .build(),
            maxAttemps,
            config);

    // Perform operations on the cluster
    jedisCluster.set("myKey", "Hello, Redis Cluster!");
    String value = jedisCluster.get("myKey");
    System.out.println("Value for myKey: " + value);

    // Disconnect from the cluster
    int count = 0;
    for (int i = 0; i < 1000; i++) {
      String k = "jediskey" + String.valueOf(i);
      String v = "jedisvalue" + String.valueOf(i);
      jedisCluster.set(k, v);
      String got = jedisCluster.get(k);
      if (got.equals(v)) {
        count++;
      } else {
        System.out.println("unexpected value");
      }
    }
    System.out.println("Successfully got " + String.valueOf(count) + " keys");
    jedisCluster.close();
    ((Closeable) credentialsProvider).close();
  }
}
