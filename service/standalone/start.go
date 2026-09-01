/*
 * Copyright © 2023 Clyso GmbH
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
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/alicebob/miniredis/v2"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/features"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/util"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/worker"
)

// redisProbeName prefixes the connection name of the startup checks.
const redisProbeName = "chorus-startup-"

func Start(ctx context.Context, app dom.AppInfo, conf *Config, flushRedis bool) error {
	// detect fake s3 storages in config
	fake := map[string]int{}
	for name, storage := range conf.Storage.Storages {
		var err error
		isFake := false
		fakePort := 0
		if !storage.Address.IsSet() {
			_, fakePort, err = getRandomPort()
			if err != nil {
				return fmt.Errorf("%w: unable to get random port", err)
			}
			isFake = true
		} else if strings.HasPrefix(storage.Address.Value(), ":") {
			fakePort, err = strconv.Atoi(strings.TrimPrefix(storage.Address.Value(), ":"))
			if err != nil {
				return fmt.Errorf("%w: unable to parse storage address %s", err, storage.Address.RawValue())
			}
			isFake = true
		}
		if isFake {
			fake[name] = fakePort
			storage.Address = s3.NewConfAddr(httpLocalhost(fakePort))
			storage.IsSecure = false
			conf.Storage.Storages[name] = storage
		}
	}

	logger, err := log.GetLogger(conf.Log, "", "")
	if err != nil {
		return err
	}
	if err := resolveAccessKeys(ctx, conf, logger); err != nil {
		return err
	}

	// validate config
	if err := conf.Validate(); err != nil {
		return err
	}
	features.Set(conf.Features)
	logger.Info().
		Str("version", app.Version).
		Str("commit", app.Commit).
		Msg("app starting...")

	// start embedded redis only when no external redis is configured:
	redisAddrs := conf.Redis.GetAddresses()
	if flushRedis && len(redisAddrs) != 0 {
		// A flush wipes whole databases, so every other user of one of them
		// loses its state. Asking redis who is connected to them catches an
		// instance that shares the redis server under its own db numbers,
		// which nothing we keep in our own databases can see.
		used, usedErr := redisDBsInUse(ctx, conf)
		if usedErr != nil {
			return usedErr
		}
		if len(used) != 0 {
			return fmt.Errorf("%w: refusing to flush redis, db %v in use by another instance", dom.ErrInvalidArg, used)
		}
	}
	var redisSvc *miniredis.Miniredis
	if len(redisAddrs) == 0 {
		var miniErr error
		redisSvc, miniErr = miniredis.Run()
		if miniErr != nil {
			return fmt.Errorf("%w: unable to start redis", miniErr)
		}
		go func() {
			<-ctx.Done()
			redisSvc.Close()
		}()
		redisAddrs = []string{redisSvc.Addr()}
	}

	if flushRedis {
		if err := flushRedisDBs(ctx, conf); err != nil {
			return err
		}
	}

	// start fake s3 storages
	g, ctx := errgroup.WithContext(ctx)
	for _, fakePort := range fake {
		port := fakePort
		g.Go(func() error {
			return serveFakeS3(ctx, port)
		})
	}

	proxyURL := "<disabled in config>"
	if conf.Proxy.Enabled {
		proxyURL = httpLocalhost(conf.Proxy.Port)
	}

	workerConf := conf.Config
	if len(workerConf.Redis.Addresses) == 0 {
		workerConf.Redis.Addresses = s3.NewConfAddrs(redisAddrs...)
	}

	// deep copy worker config
	wcBytes, err := yaml.Marshal(&workerConf)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(wcBytes, &workerConf)
	if err != nil {
		return err
	}
	// start worker — the worker subsystem owns the single metrics/pprof
	// listener in standalone; proxy.Start is invoked with serveMetrics=false
	// so the two don't fight over the same port.
	g.Go(func() error {
		return worker.Start(ctx, app, &workerConf, true)
	})

	if conf.Proxy.Enabled {
		proxyConf := proxy.Config{
			Common:              conf.Common,
			Auth:                conf.Proxy.Auth,
			Port:                conf.Proxy.Port,
			Address:             conf.Proxy.Address,
			Storage:             conf.Storage,
			Cors:                conf.Proxy.Cors,
			ReadFromDestination: conf.Proxy.ReadFromDestination,
		}
		if len(proxyConf.Redis.Addresses) == 0 {
			proxyConf.Redis.Addresses = s3.NewConfAddrs(redisAddrs...)
		}

		// deep copy proxy config
		pcBytes, err := yaml.Marshal(&proxyConf)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(pcBytes, &proxyConf)
		if err != nil {
			return err
		}
		// start proxy without its own metrics listener (see worker above).
		g.Go(func() error {
			return proxy.Start(ctx, app, &proxyConf, false)
		})
	}

	uiURL := ""
	uiServer, err := serveUI(ctx, conf.UIPort)
	if err == nil {
		// start UI
		g.Go(func() error {
			return uiServer()
		})
		uiURL = httpLocalhost(conf.UIPort)
	} else if !errors.Is(err, dom.ErrNotFound) {
		return err
	}

	_, useFakeStorageCreds := fake[conf.Proxy.Auth.UseStorage]

	httpPort := httpLocalhost(conf.Api.HttpPort)
	if conf.Api.Secure {
		httpPort = "disabled due to grpc TLS"
	}
	ev := logger.Info().
		Str("s3_proxy_url", proxyURL).
		Array("s3_proxy_credentials", credsList(conf, useFakeStorageCreds)).
		Str("grpc_api", localhost(conf.Api.GrpcPort)).
		Str("http_api", httpPort).
		Strs("redis", redisAddrs).
		Array("storages", storageList(fake, conf.Storage))
	if uiURL != "" {
		ev = ev.Str("mgmt_ui_url", uiURL)
	}
	ev.Msg("standalone services started")

	return g.Wait()
}

func httpLocalhost(port int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

func localhost(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func credsList(conf *Config, printSecrets bool) *zerolog.Array {
	arr := zerolog.Arr()
	if !conf.Proxy.Enabled {
		return arr
	}
	var creds map[string]s3.CredentialsV4
	if conf.Proxy.Auth.UseStorage != "" {
		creds = conf.Storage.Storages[conf.Proxy.Auth.UseStorage].Credentials
	} else {
		creds = conf.Proxy.Auth.Custom
	}
	for name, v4 := range creds {
		secret := "<hidden>"
		if printSecrets {
			secret = v4.SecretAccessKey
		}
		arr = arr.Dict(zerolog.Dict().
			Str("name", name).
			Str("access_key", v4.AccessKeyID).
			Str("secret_key", secret))
	}
	return arr
}

func storageList(fake map[string]int, conf *s3.StorageConfig) *zerolog.Array {
	arr := zerolog.Arr()
	for name, stor := range conf.Storages {
		_, isFake := fake[name]
		arr = arr.Dict(zerolog.Dict().
			Str("name", name).
			Str("address", stor.Address.ValueWithProtocol()).
			Bool("fake", isFake).
			Bool("main", stor.IsMain))
	}
	return arr
}

// getRedisDBs returns the databases this instance uses, without duplicates.
func getRedisDBs(conf *Config) []int {
	seen := map[int]struct{}{}
	dbs := []int{conf.Redis.MetaDB, conf.Redis.QueueDB, conf.Redis.LockDB, conf.Redis.ConfigDB}
	dedup := make([]int, 0, len(dbs))
	for _, db := range dbs {
		if _, ok := seen[db]; ok {
			continue
		}
		seen[db] = struct{}{}
		dedup = append(dedup, db)
	}
	return dedup
}

func flushRedisDBs(ctx context.Context, conf *Config) error {
	dbs := getRedisDBs(conf)
	flushed := make([]int, 0, len(dbs))
	for _, db := range dbs {
		client := util.NewRedis(conf.Redis, db)
		// FLUSHDB ASYNC lets Redis reclaim the keyspace in a background thread and
		// acks immediately, so a large synchronous flush cannot exceed the client
		// read timeout and abort startup. Keys are gone at once; memory is freed
		// in the background before services (which start after this) write to Redis.
		err := client.FlushDBAsync(ctx).Err()
		_ = client.Close()
		if err != nil {
			return fmt.Errorf("flush redis db %d: %w", db, err)
		}
		flushed = append(flushed, db)
	}
	zerolog.Ctx(ctx).Info().Ints("dbs", flushed).Msg("flushed redis databases")
	return nil
}

func getRandomPort() (string, int, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", 0, err
	}
	addr := l.Addr().String()
	addrs := strings.Split(addr, ":")
	err = l.Close()
	if err != nil {
		return "", 0, err
	}

	port, err := strconv.Atoi(addrs[len(addrs)-1])
	if err != nil {
		return "", 0, err
	}
	return addr, port, nil
}

// redisDBsInUse returns those of the databases we would flush that another
// client is connected to. Own connections are recognised by their name.
func redisDBsInUse(ctx context.Context, conf *Config) ([]int, error) {
	name := fmt.Sprintf("%s%d", redisProbeName, os.Getpid())
	client := util.NewRedisNamed(conf.Redis, conf.Redis.QueueDB, name)
	defer client.Close()
	list, err := client.ClientList(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to list redis clients: %w", err)
	}

	ours := map[int]struct{}{}
	for _, db := range getRedisDBs(conf) {
		ours[db] = struct{}{}
	}
	seen := map[int]struct{}{}
	used := make([]int, 0, len(ours))
	for _, line := range strings.Split(list, "\n") {
		db, isOurs := -1, false
		for _, field := range strings.Fields(line) {
			switch key, value, _ := strings.Cut(field, "="); key {
			case "db":
				db, _ = strconv.Atoi(value)
			case "name":
				isOurs = value == name
			}
		}
		if _, ok := ours[db]; !ok || isOurs {
			continue
		}
		if _, ok := seen[db]; ok {
			continue
		}
		seen[db] = struct{}{}
		used = append(used, db)
	}
	sort.Ints(used)
	return used, nil
}
