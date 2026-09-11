package standalone

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/service/proxy"
	"github.com/clyso/chorus/service/proxy/auth"
	"github.com/clyso/chorus/service/proxy/cors"
)

// Every option the proxy has of its own, set to something recognisable.
func standaloneConf() *Config {
	conf := &Config{}
	conf.Redis = &config.Redis{Addresses: s3.NewConfAddrs("top-level:6379")}
	conf.Storage = &s3.StorageConfig{DefaultRegion: "top-level-region"}
	conf.Proxy = proxy.Config{
		Enabled:             true,
		Port:                9669,
		Address:             s3.NewConfAddr("http://localhost:9669"),
		Auth:                &auth.Config{},
		Cors:                &cors.Config{Enabled: true},
		ReadFromDestination: true,
	}
	return conf
}

func TestProxyConfig(t *testing.T) {
	t.Run("The options of the proxy section reach the proxy", func(t *testing.T) {
		r := require.New(t)

		got, err := proxyConfig(standaloneConf(), nil)
		r.NoError(err)
		r.EqualValues(9669, got.Port)
		r.True(got.ReadFromDestination)
	})

	t.Run("What the process has only one of comes from the top level", func(t *testing.T) {
		r := require.New(t)

		conf := standaloneConf()
		got, err := proxyConfig(conf, nil)
		r.NoError(err)
		r.Equal("top-level-region", got.Storage.DefaultRegion)
		r.Equal([]string{"top-level:6379"}, got.Redis.GetAddresses())
	})

	t.Run("An embedded redis is handed to a proxy that has none", func(t *testing.T) {
		r := require.New(t)

		conf := standaloneConf()
		conf.Redis = &config.Redis{}
		got, err := proxyConfig(conf, []string{"embedded:6379"})
		r.NoError(err)
		r.Equal([]string{"embedded:6379"}, got.Redis.GetAddresses())
	})

	// Worker and proxy start side by side and each validates its config, which
	// writes to it; anything they share they would write to concurrently.
	t.Run("Worker and proxy get a config of their own", func(t *testing.T) {
		r := require.New(t)

		conf := standaloneConf()
		conf.Redis = &config.Redis{}
		conf.Storage.Storages = map[string]s3.Storage{"main": {IsMain: true}}

		pc, err := proxyConfig(conf, []string{"embedded:6379"})
		r.NoError(err)
		wc, err := workerConfig(conf, []string{"embedded:6379"})
		r.NoError(err)

		r.Empty(conf.Redis.Addresses, "the embedded redis was written into the shared config")
		for _, own := range []struct {
			storage *s3.StorageConfig
			redis   *config.Redis
		}{{pc.Storage, pc.Redis}, {wc.Storage, wc.Redis}} {
			r.NotSame(conf.Storage, own.storage)
			r.NotSame(conf.Redis, own.redis)
			own.storage.Storages["main"] = s3.Storage{}
		}
		r.NotSame(pc.Storage, wc.Storage)
		r.NotSame(pc.Redis, wc.Redis)
		r.NotSame(conf.Proxy.Auth, pc.Auth)
		r.NotSame(conf.Proxy.Cors, pc.Cors)
		r.True(conf.Storage.Storages["main"].IsMain, "a write to a copy's storages reached the shared map")
	})

}
