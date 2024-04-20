// Example of Go-Redis application Client Library to connect to
// Memorystore Redis Cluster with Authentication and TLS enabled

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"time"

	credentials "google.golang.org/genproto/googleapis/iam/credentials/v1"

	"github.com/golang/protobuf/ptypes"
	"github.com/redis/go-redis/v9"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/grpc"
)

var (
	svcAccount = flag.String("a", "aoguo-test-sa@aoguo-test.iam.gserviceaccount.com", "service account email")
	lifetime   = flag.Duration("d", time.Hour, "lifetime of token")
)

func retrieveToken() string {
	ctx := context.Background()
	conn, err := gtransport.Dial(ctx,
		option.WithEndpoint("iamcredentials.googleapis.com:443"),
		option.WithScopes("https://www.googleapis.com/auth/cloud-platform"))

	if err != nil {
		log.Fatal("Failed to dial API: ", err)
	}
	client := credentials.NewIAMCredentialsClient(conn)
	req := credentials.GenerateAccessTokenRequest{
		Name:     path.Join("projects/-/serviceAccounts", *svcAccount),
		Scope:    []string{"https://www.googleapis.com/auth/cloud-platform"},
		Lifetime: ptypes.DurationProto(*lifetime),
	}
	rsp, err := client.GenerateAccessToken(ctx, &req)
	if err != nil {
		log.Fatal("Failed to call GenerateAccessToken: ", err)
	}
	return rsp.AccessToken
}

func retrieveTokenFunc() (string, string) {
	username := "default"
	password := retrieveToken()
	return username, password
}

func main() {
	// Load CA cert
	caFilePath := "server-ca.pem"
	caCert, err := ioutil.ReadFile(caFilePath)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup Redis Connection pool
	client := redis.NewClient(&redis.Options{
		Addr: "10.142.0.12:6379",
		// PoolSize applies per cluster node and not for the whole cluster.
		PoolSize:            10,
		ConnMaxIdleTime:     60 * time.Second,
		MinIdleConns:        1,
		CredentialsProvider: retrieveTokenFunc,
		TLSConfig: &tls.Config{
			RootCAs: caCertPool,
		},
	})

	ctx := context.Background()
	err = client.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		log.Fatal(err)
	}
	val, err := client.Get(ctx, "key").Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Got the value for key: key, which is %s \n", val)
}
