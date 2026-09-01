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
	"github.com/clyso/chorus/pkg/listen"
	"github.com/clyso/chorus/pkg/s3"
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
// not share our redis, so it is not a rollout of ours.
func TestForeignPortRefused(t *testing.T) {
	r := require.New(t)
	conf, err := GetConfig()
	r.NoError(err)
	conf.ReusePort = true
	// a redis of our own, so that an instance of another test or of a
	// previous run cannot pass as the owner of the port
	conf.Redis.MetaDB, conf.Redis.QueueDB = 12, 13
	conf.Redis.LockDB, conf.Redis.ConfigDB = 14, 15

	// something else already listens on the api port
	foreign, err := net.Listen("tcp", localhost(conf.Api.GrpcPort))
	r.NoError(err)
	defer foreign.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = Start(ctx, dom.AppInfo{}, conf, false)
	r.ErrorIs(err, dom.ErrInvalidArg)
	r.Contains(err.Error(), "does not share our redis")
}

// TestPortOwnerOfOtherBuckets covers what tells two migrations apart: the
// buckets they move. Being registered on our redis only says that some
// instance of ours is alive, which a just killed one is for a few seconds.
func TestPortOwnerOfOtherBuckets(t *testing.T) {
	r := require.New(t)
	conf, err := GetConfig()
	r.NoError(err)
	// databases of our own, so that our side of the comparison is empty
	conf.Redis.MetaDB, conf.Redis.QueueDB = 12, 13
	conf.Redis.LockDB, conf.Redis.ConfigDB = 14, 15

	serve := func(replications []*pb.Replication) func() {
		listener, err := net.Listen("tcp", localhost(conf.Api.GrpcPort))
		r.NoError(err)
		srv := grpc.NewServer()
		pb.RegisterChorusServer(srv, &replicationLister{replications: replications})
		go func() { _ = srv.Serve(listener) }()
		return srv.Stop
	}

	stop := serve(nil)
	same, err := replicationsMatch(t.Context(), conf)
	stop()
	r.NoError(err)
	r.True(same, "an instance that replicates nothing either is ours")

	stop = serve([]*pb.Replication{{User: "user", From: "one", Bucket: "other-bucket"}})
	defer stop()
	same, err = replicationsMatch(t.Context(), conf)
	r.NoError(err)
	r.False(same, "another migration holds the port")
}

// replicationLister is the part of the api the port owner is asked for.
type replicationLister struct {
	pb.UnimplementedChorusServer
	replications []*pb.Replication
}

func (l *replicationLister) ListReplications(_ context.Context, _ *emptypb.Empty) (*pb.ListReplicationsResponse, error) {
	return &pb.ListReplicationsResponse{Replications: l.replications}, nil
}
