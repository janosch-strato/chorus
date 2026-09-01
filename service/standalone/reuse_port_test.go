/*
 * Copyright © 2026 STRATO GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package standalone

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/listen"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/util"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
)

// TestOverlappingInstances covers the rollout reusePort exists for: a second
// instance binds the same ports while the first still serves, and the ports
// keep answering when the first one stops.
func TestOverlappingInstances(t *testing.T) {
	r := require.New(t)
	// main sets this before anything runs, the other tests of this package
	// run in the same process and expect the default
	listen.SetReusePort(true)
	t.Cleanup(func() { listen.SetReusePort(false) })

	newConf := func() *Config {
		conf, err := GetConfig()
		r.NoError(err)
		conf.ReusePort = true
		conf.ShutdownTimeout = 500 * time.Millisecond
		// fake storages get a random port per instance, they cannot share
		for name, storage := range conf.Storage.Storages {
			storage.Address = s3.ConfAddr{}
			conf.Storage.Storages[name] = storage
		}
		return conf
	}
	confA, confB := newConf(), newConf()

	start := func(conf *Config) (context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- Start(ctx, dom.AppInfo{}, conf, false) }()
		return cancel, done
	}

	// the fake storages of an instance listen on ports of their own, so the
	// addresses in an answer say which instance served the call
	serverID := func(what string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", confA.Api.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return "", fmt.Errorf("%s: dial: %w", what, err)
		}
		defer conn.Close()
		res, err := pb.NewChorusClient(conn).GetStorages(ctx, &emptypb.Empty{})
		if err != nil {
			return "", fmt.Errorf("%s: call: %w", what, err)
		}
		if len(res.Storages) == 0 {
			return "", fmt.Errorf("%s: no storages", what)
		}
		addrs := make([]string, 0, len(res.Storages))
		for _, storage := range res.Storages {
			addrs = append(addrs, storage.Address)
		}
		sort.Strings(addrs)
		return strings.Join(addrs, ","), nil
	}

	cancelA, doneA := start(confA)
	var instanceA string
	r.Eventually(func() bool {
		var err error
		instanceA, err = serverID("instance A")
		return err == nil
	}, 20*time.Second, 200*time.Millisecond, "instance A did not come up")
	t.Log("instance A serving")

	// the second instance binds the same ports while the first still serves.
	// The kernel hands a connection to one of them, so poll until an answer
	// comes from B.
	cancelB, doneB := start(confB)
	defer func() { cancelB(); <-doneB }()
	r.Eventually(func() bool {
		select {
		case err := <-doneB:
			r.NoError(err, "instance B did not start")
		default:
		}
		id, err := serverID("both instances")
		return err == nil && id != instanceA
	}, 20*time.Second, 200*time.Millisecond, "instance B did not come up")
	t.Log("instance B serving alongside A")

	// the rollout: stop the old instance, the port keeps answering
	cancelA()
	select {
	case err := <-doneA:
		t.Logf("instance A stopped: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("instance A did not stop")
	}
	for i := range 20 {
		_, err := serverID(fmt.Sprintf("after A stopped, call %d", i))
		r.NoError(err)
		time.Sleep(50 * time.Millisecond)
	}
	t.Log("port kept serving through the handover")
}

// TestForeignPortRefused covers the accident reusePort would otherwise hide: a
// deployment configured with the ports of another one. The other instance does
// not share our redis, so it is not a rollout of ours. Every port we listen on
// is one we can be configured onto by mistake.
func TestForeignPortRefused(t *testing.T) {
	for name, ourPort := range map[string]func(*Config) *int{
		"grpc api": func(c *Config) *int { return &c.Api.GrpcPort },
		"http api": func(c *Config) *int { return &c.Api.HttpPort },
		"proxy":    func(c *Config) *int { return &c.Proxy.Port },
		"ui":       func(c *Config) *int { return &c.UIPort },
		"metrics":  func(c *Config) *int { return &c.Metrics.Port },
	} {
		t.Run(name, func(t *testing.T) {
			r := require.New(t)
			conf, err := GetConfig()
			r.NoError(err)
			conf.ReusePort = true
			conf.Metrics.Enabled = true
			// a redis and ports of our own, so that an instance of another
			// test cannot pass as the owner of the port
			conf.Redis.MetaDB, conf.Redis.QueueDB = 12, 13
			conf.Redis.LockDB, conf.Redis.ConfigDB = 14, 15
			for _, port := range []*int{&conf.Api.GrpcPort, &conf.Api.HttpPort,
				&conf.Proxy.Port, &conf.UIPort, &conf.Metrics.Port} {
				_, free, err := getRandomPort()
				r.NoError(err)
				*port = free
			}

			foreign, err := net.Listen("tcp", localhost(*ourPort(conf)))
			r.NoError(err)
			defer foreign.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err = Start(ctx, dom.AppInfo{}, conf, false)
			r.ErrorIs(err, dom.ErrInvalidArg)
			r.Contains(err.Error(), "does not share our redis")
		})
	}
}

func Test_configuredPorts(t *testing.T) {
	r := require.New(t)
	conf, err := GetConfig()
	r.NoError(err)
	r.Equal([]int{conf.Api.GrpcPort, conf.Api.HttpPort, conf.Proxy.Port, conf.UIPort},
		configuredPorts(conf))

	conf.Api.Secure = true
	conf.Api.Status.Enabled = true
	conf.Proxy.Enabled = false
	conf.Metrics.Enabled = true
	r.Equal([]int{conf.Api.GrpcPort, conf.Api.Status.Port, conf.Metrics.Port, conf.UIPort},
		configuredPorts(conf), "no http gateway for a tls api, no port for a disabled proxy")

	conf.Api.Enabled = false
	r.Equal([]int{conf.Metrics.Port, conf.UIPort}, configuredPorts(conf))
}

// TestMigrationMatchesConfig covers what tells two migrations apart: the
// buckets they move, as our own config names them.
func TestMigrationMatchesConfig(t *testing.T) {
	r := require.New(t)
	conf, err := GetConfig()
	r.NoError(err)
	// databases of our own, so that they hold no replication of anyone
	conf.Redis.MetaDB, conf.Redis.QueueDB = 12, 13
	conf.Redis.LockDB, conf.Redis.ConfigDB = 14, 15
	conf.Storage.BucketMapping = map[string]map[string]string{
		"two": {"our-bucket": "our-bucket-renamed"},
	}
	ours := []*pb.Replication{{User: "user", From: "one", Bucket: "our-bucket", ToBucket: "our-bucket-renamed"}}
	otherSource := []*pb.Replication{{User: "user", From: "one", Bucket: "other-bucket", ToBucket: "our-bucket-renamed"}}
	otherDestination := []*pb.Replication{{User: "user", From: "one", Bucket: "our-bucket", ToBucket: "other-bucket"}}

	serve := func(t *testing.T, replications []*pb.Replication) {
		listener, err := net.Listen("tcp", localhost(conf.Api.GrpcPort))
		r.NoError(err)
		srv := grpc.NewServer()
		pb.RegisterChorusServer(srv, &replicationLister{replications: replications})
		go func() { _ = srv.Serve(listener) }()
		t.Cleanup(srv.Stop)
	}

	seed := func(t *testing.T, fromBucket, toBucket string) {
		client := util.NewRedis(conf.Redis, conf.Redis.ConfigDB)
		id := entity.NewReplicationStatusID("user", "one", fromBucket, "two", toBucket)
		status := entity.ReplicationStatus{CreatedAt: time.Now().UTC()}
		r.NoError(store.NewReplicationStatusStore(client).Set(t.Context(), id, status))
		t.Cleanup(func() {
			_ = client.FlushDB(context.Background()).Err()
			_ = client.Close()
		})
	}

	t.Run("the instance and the databases are about our buckets", func(t *testing.T) {
		serve(t, ours)
		seed(t, "our-bucket", "our-bucket-renamed")

		require.NoError(t, checkMigrationMatchesConfig(t.Context(), conf, "port"))
	})

	t.Run("the instance is about another source bucket", func(t *testing.T) {
		serve(t, otherSource)

		err := checkMigrationMatchesConfig(t.Context(), conf, "port")
		require.ErrorIs(t, err, dom.ErrInvalidArg)
		require.Contains(t, err.Error(), "check the ports")
	})

	t.Run("the instance is about another destination bucket", func(t *testing.T) {
		serve(t, otherDestination)

		err := checkMigrationMatchesConfig(t.Context(), conf, "port")
		require.ErrorIs(t, err, dom.ErrInvalidArg)
		require.Contains(t, err.Error(), "check the ports")
	})

	t.Run("the databases are about another destination bucket", func(t *testing.T) {
		serve(t, ours)
		seed(t, "our-bucket", "other-bucket")

		err := checkMigrationMatchesConfig(t.Context(), conf, "port")
		require.ErrorIs(t, err, dom.ErrInvalidArg)
		require.Contains(t, err.Error(), "check the db numbers")
	})

	t.Run("neither has a replication yet", func(t *testing.T) {
		serve(t, nil)

		require.NoError(t, checkMigrationMatchesConfig(t.Context(), conf, "port"))
	})

	t.Run("nothing answers our port", func(t *testing.T) {
		err := checkMigrationMatchesConfig(t.Context(), conf, "port")
		require.ErrorIs(t, err, dom.ErrInvalidArg)
		require.Contains(t, err.Error(), "did not identify itself")
	})

	t.Run("a config without bucket mapping is left to the instance check", func(t *testing.T) {
		serve(t, otherSource)
		unmapped := *conf
		unmapped.Storage = &s3.StorageConfig{}

		require.NoError(t, checkMigrationMatchesConfig(t.Context(), &unmapped, "port"))
	})
}

func Test_configuredMigrations(t *testing.T) {
	r := require.New(t)
	conf, err := GetConfig()
	r.NoError(err)
	r.Empty(configuredMigrations(conf), "a config without a bucket mapping names none")

	conf.Storage.BucketMapping = map[string]map[string]string{
		"two":   {"second": "renamed", "first": "first"},
		"three": {"first": "first"},
	}
	r.Equal([]string{"first>first", "second>renamed"}, configuredMigrations(conf))
}

func Test_sharesMigration(t *testing.T) {
	r := require.New(t)
	ours := []string{"first>first", "second>renamed"}

	r.True(sharesMigration(ours, nil), "nobody without a replication belongs to another migration")
	r.True(sharesMigration(ours, []string{"second>renamed"}))
	r.False(sharesMigration(ours, []string{"second>elsewhere"}), "the destination is part of it")
	r.False(sharesMigration(ours, []string{"third>third"}))
}

// replicationLister is the part of the api the port owner is asked for.
type replicationLister struct {
	pb.UnimplementedChorusServer
	replications []*pb.Replication
}

func (l *replicationLister) ListReplications(_ context.Context, _ *emptypb.Empty) (*pb.ListReplicationsResponse, error) {
	return &pb.ListReplicationsResponse{Replications: l.replications}, nil
}

// TestForeignPortWithOurRedis covers the accident through Start: an instance of
// ours is registered on our redis, so the registration cannot tell that the
// ports our config names belong to another migration.
func TestForeignPortWithOurRedis(t *testing.T) {
	r := require.New(t)
	listen.SetReusePort(true)
	t.Cleanup(func() { listen.SetReusePort(false) })

	newConf := func() *Config {
		conf, err := GetConfig()
		r.NoError(err)
		conf.ReusePort = true
		conf.ShutdownTimeout = 500 * time.Millisecond
		// databases of our own, apart from those of the other tests
		conf.Redis.MetaDB, conf.Redis.QueueDB = 8, 9
		conf.Redis.LockDB, conf.Redis.ConfigDB = 10, 11
		// fake storages get a random port per instance, they cannot share
		for name, storage := range conf.Storage.Storages {
			storage.Address = s3.ConfAddr{}
			conf.Storage.Storages[name] = storage
		}
		return conf
	}

	// an instance of ours, on our redis but on ports of its own
	running := newConf()
	for _, port := range []*int{&running.Api.GrpcPort, &running.Api.HttpPort,
		&running.Proxy.Port, &running.UIPort, &running.Metrics.Port} {
		_, free, err := getRandomPort()
		r.NoError(err)
		*port = free
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Start(ctx, dom.AppInfo{}, running, false) }()
	defer func() { cancel(); <-done }()
	r.Eventually(func() bool {
		alive, err := instanceRunning(running)
		return err == nil && alive
	}, 20*time.Second, 200*time.Millisecond, "our instance did not register")

	// another migration is on the ports our config names
	conf := newConf()
	conf.Storage.BucketMapping = map[string]map[string]string{
		"two": {"our-bucket": "our-bucket"},
	}
	listener, err := net.Listen("tcp", localhost(conf.Api.GrpcPort))
	r.NoError(err)
	defer listener.Close()
	srv := grpc.NewServer()
	pb.RegisterChorusServer(srv, &replicationLister{
		replications: []*pb.Replication{{User: "user", From: "one", Bucket: "other-bucket"}},
	})
	go func() { _ = srv.Serve(listener) }()
	defer srv.Stop()

	err = Start(context.Background(), dom.AppInfo{}, conf, false)
	r.ErrorIs(err, dom.ErrInvalidArg)
	r.Contains(err.Error(), "check the ports")
}

// TestFreshStartWithNoInstance guards the other side of the checks above: a
// deployment on free ports has nothing to be compared against and comes up.
func TestFreshStartWithNoInstance(t *testing.T) {
	r := require.New(t)
	conf, err := GetConfig()
	r.NoError(err)
	conf.ShutdownTimeout = 500 * time.Millisecond
	// databases and ports nobody else in this package uses
	conf.Redis.MetaDB, conf.Redis.QueueDB = 4, 5
	conf.Redis.LockDB, conf.Redis.ConfigDB = 6, 7
	for _, port := range []*int{&conf.Api.GrpcPort, &conf.Api.HttpPort,
		&conf.Proxy.Port, &conf.UIPort, &conf.Metrics.Port} {
		_, free, err := getRandomPort()
		r.NoError(err)
		*port = free
	}
	for name, storage := range conf.Storage.Storages {
		storage.Address = s3.ConfAddr{}
		conf.Storage.Storages[name] = storage
	}
	conf.Storage.BucketMapping = map[string]map[string]string{
		"two": {"our-bucket": "our-bucket-renamed"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	// flushing is part of a fresh start, and nobody else is on those databases
	go func() { done <- Start(ctx, dom.AppInfo{}, conf, true) }()
	defer func() { cancel(); <-done }()

	r.Eventually(func() bool {
		select {
		case err := <-done:
			r.NoError(err, "the instance did not start")
		default:
		}
		callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
		defer callCancel()
		conn, err := grpc.NewClient(localhost(conf.Api.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return false
		}
		defer conn.Close()
		res, err := pb.NewChorusClient(conn).GetStorages(callCtx, &emptypb.Empty{})
		return err == nil && len(res.Storages) != 0
	}, 20*time.Second, 200*time.Millisecond, "the instance never served")
}
