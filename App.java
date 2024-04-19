import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.protobuf.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.DefaultRedisCredentials;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.RedisCredentials;
import redis.clients.jedis.RedisCredentialsProvider;

public class App {

  private static final class RedisClusterCredentialsProvider implements RedisCredentialsProvider {
    @Override
    public RedisCredentials get() {
      String password = "";
    // To Do: Please modify the Service Account used, 'example-service-account@example-project.iam.gserviceaccount.com' to access the MRC. 
    // The service account must have "Cloud Memorystore Redis Db Connection User"
      
      try (IamCredentialsClient iamCredentialsClient = IamCredentialsClient.create()) {
        String name =
            "projects/-/serviceAccounts/example-service-account@example-project.iam.gserviceaccount.com";
        List<String> delegates = new ArrayList<>();
        List<String> scopes = new ArrayList<>();
        scopes.add("https://www.googleapis.com/auth/cloud-platform");
        Duration lifetime = Duration.newBuilder().setSeconds(3600).build();
        GenerateAccessTokenResponse response =
            iamCredentialsClient.generateAccessToken(name, delegates, scopes, lifetime);
        password = response.getAccessToken();
        return new DefaultRedisCredentials("default", password);
      } catch (Exception ex) {
        // exception handling
        ex.printStackTrace();
        return null;
      }
    }
  }

  public static void main(String[] args) {
    String discoveryEndpointIp = "insert discovery endpoint ip";
    int discoveryEndpointPort = 6379;

    GenericObjectPoolConfig<Connection> config = new GenericObjectPoolConfig<Connection>();
    config.setTestWhileIdle(true);
    int timeout = 5000;
    int maxAttemps = 5;
    HostAndPort discovery = new HostAndPort(discoveryEndpointIp, discoveryEndpointPort);
    RedisCredentialsProvider credentialsProvider = new RedisClusterCredentialsProvider();

    // Create JedisCluster instance
    JedisCluster jedisCluster =
        new JedisCluster(
            discovery,
            DefaultJedisClientConfig.builder()
                .timeoutMillis(timeout)
                .credentialsProvider(credentialsProvider)
                .build(),
            maxAttemps,
            config);

    // Perform operations on the cluster
    jedisCluster.set("myKey", "Hello, Redis Cluster!");
    String value = jedisCluster.get("myKey");
    System.out.println("Value for myKey: " + value);

    // Disconnect from the cluster
    jedisCluster.close();
  }
}
