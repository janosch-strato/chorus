package s3

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStorageConfig_Validate(t *testing.T) {
	s := StorageConfig{
		Storages: map[string]Storage{
			"a": {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"b": {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"c": {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"d": {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"e": {IsMain: true, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"f": {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			"g": {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
		},
	}
	r := require.New(t)
	r.NoError(s.Init())
	res1 := make([]string, len(s.Storages))
	copy(res1, s.storageList)
	r.NoError(s.Init())
	res2 := make([]string, len(s.Storages))
	copy(res2, s.storageList)
	r.NoError(s.Init())
	res3 := make([]string, len(s.Storages))
	copy(res3, s.storageList)
	r.EqualValues(res1, res2)
	r.EqualValues(res3, res2)

	r.EqualValues(res1[0], "e")
	r.EqualValues(res1[1], "a")
	r.EqualValues(res1[2], "b")
	r.EqualValues(res1[3], "c")
	r.EqualValues(res1[4], "d")
	r.EqualValues(res1[5], "f")
	r.EqualValues(res1[6], "g")

	fol := s.Followers()
	r.EqualValues(fol[0], "a")
	r.EqualValues(fol[1], "b")
	r.EqualValues(fol[2], "c")
	r.EqualValues(fol[3], "d")
	r.EqualValues(fol[4], "f")
	r.EqualValues(fol[5], "g")
	r.EqualValues(len(fol), len(res1)-1)
}

func TestStorageConfig_ValidateIdentifiers(t *testing.T) {
	r := require.New(t)
	s := StorageConfig{
		Storages: map[string]Storage{
			"a:b": {IsMain: true, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
		},
	}
	r.Error(s.Init(), "storage name must not contain ':'")

	s = StorageConfig{
		Storages: map[string]Storage{
			"a": {IsMain: true, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"us:er": {"1", "2"}}},
		},
	}
	r.Error(s.Init(), "user name must not contain ':'")
}

func TestStorageConfig_ValidateAddress(t *testing.T) {
	t.Run("Add http", func(t *testing.T) {
		r := require.New(t)

		s := StorageConfig{
			Storages: map[string]Storage{
				"a": {IsMain: true, Address: NewConfAddr("clyso.com"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
		}
		r.NoError(s.Init())
		r.EqualValues("http://clyso.com", s.Storages["a"].Address.ValueWithProtocol())
	})
	t.Run("Add https", func(t *testing.T) {
		r := require.New(t)

		s := StorageConfig{
			Storages: map[string]Storage{
				"a": {IsMain: true, IsSecure: true, Address: NewConfAddr("clyso.com"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
		}
		r.NoError(s.Init())
		r.EqualValues("https://clyso.com", s.Storages["a"].Address.ValueWithProtocol())
	})

	t.Run("Already http", func(t *testing.T) {
		r := require.New(t)

		s := StorageConfig{
			Storages: map[string]Storage{
				"a": {IsMain: true, Address: NewConfAddr("http://clyso.com"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
		}
		r.NoError(s.Init())
		r.EqualValues("http://clyso.com", s.Storages["a"].Address.ValueWithProtocol())
	})
	t.Run("Already https", func(t *testing.T) {
		r := require.New(t)

		s := StorageConfig{
			Storages: map[string]Storage{
				"a": {IsMain: true, IsSecure: true, Address: NewConfAddr("https://clyso.com"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
		}
		r.NoError(s.Init())
		r.EqualValues("https://clyso.com", s.Storages["a"].Address.ValueWithProtocol())
	})

	t.Run("Invalid http", func(t *testing.T) {
		r := require.New(t)

		s := StorageConfig{
			Storages: map[string]Storage{
				"a": {IsMain: true, Address: NewConfAddr("https://clyso.com"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
		}
		r.Error(s.Init())
	})
	t.Run("Invalid https", func(t *testing.T) {
		r := require.New(t)

		s := StorageConfig{
			Storages: map[string]Storage{
				"a": {IsMain: true, IsSecure: true, Address: NewConfAddr("http://clyso.com"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
		}
		r.Error(s.Init())
	})
	t.Run("Invalid url", func(t *testing.T) {
		r := require.New(t)

		s := StorageConfig{
			Storages: map[string]Storage{
				"a": {IsMain: true, IsSecure: true, Address: NewConfAddr("http::clyso.com"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
		}
		r.Error(s.Init())
	})
}

func TestBucketMappingValidation(t *testing.T) {
	r := require.New(t)

	mainStorage := Storage{
		IsMain:      true,
		Address:     NewConfAddr("mainAddress"),
		Provider:    "p",
		Credentials: map[string]CredentialsV4{"user": {"1", "2"}},
	}

	t.Run("Invalid - Missing Storage Name", func(t *testing.T) {
		s := StorageConfig{
			Storages: map[string]Storage{
				"main": mainStorage,
				"a":    {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
			BucketMapping: map[string]map[string]string{
				"": {"sourceBucket": "destBucket"},
			},
		}
		err := s.Init()
		r.Error(err)
		r.Contains(err.Error(), "bucket name is missing")
	})

	t.Run("Invalid - Undefined Storage", func(t *testing.T) {
		s := StorageConfig{
			Storages: map[string]Storage{
				"main": mainStorage,
				"a":    {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
			BucketMapping: map[string]map[string]string{
				"missingStorage": {"sourceBucket": "destBucket"},
			},
		}
		err := s.Init()
		r.Error(err)
		r.Contains(err.Error(), "storage missingStorage is not defined")
	})

	t.Run("Invalid - Main Storage", func(t *testing.T) {
		s := StorageConfig{
			Storages: map[string]Storage{
				"main": mainStorage,
			},
			BucketMapping: map[string]map[string]string{
				"main": {"sourceBucket": "destBucket"},
			},
		}
		err := s.Init()
		r.Error(err)
		r.Contains(err.Error(), "storage main is the main storage")
	})

	t.Run("Invalid - Missing Destination Bucket", func(t *testing.T) {
		s := StorageConfig{
			Storages: map[string]Storage{
				"main": mainStorage,
				"a":    {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
			BucketMapping: map[string]map[string]string{
				"a": {"sourceBucket": ""},
			},
		}
		err := s.Init()
		r.Error(err)
		r.Contains(err.Error(), "source or destination bucket name is missing")
	})

	t.Run("Invalid - Missing Source Bucket", func(t *testing.T) {
		s := StorageConfig{
			Storages: map[string]Storage{
				"main": mainStorage,
				"a":    {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
			BucketMapping: map[string]map[string]string{
				"a": {"": "destBucket"},
			},
		}
		err := s.Init()
		r.Error(err)
		r.Contains(err.Error(), "source or destination bucket name is missing")
	})

	t.Run("Valid - Proper Bucket Mapping", func(t *testing.T) {
		s := StorageConfig{
			Storages: map[string]Storage{
				"main": mainStorage,
				"a":    {IsMain: false, Address: NewConfAddr("a"), Provider: "p", Credentials: map[string]CredentialsV4{"user": {"1", "2"}}},
			},
			BucketMapping: map[string]map[string]string{
				"a": {"sourceBucket": "destBucket"},
			},
		}
		err := s.Init()
		r.NoError(err)
	})

}

func storageConf(t *testing.T, s Storage) *StorageConfig {
	t.Helper()
	s.Provider = "Other"
	s.Address = NewConfAddr("http://storage:9000")
	s.IsMain = true
	s.Credentials = map[string]CredentialsV4{
		"user": {AccessKeyID: "ak", SecretAccessKey: "sk"},
	}
	return &StorageConfig{Storages: map[string]Storage{"one": s}}
}

func TestStorageHttpTimeout(t *testing.T) {
	// An object has no size limit, so neither has the time one may take.
	t.Run("Unset is no limit at all", func(t *testing.T) {
		r := require.New(t)

		conf := storageConf(t, Storage{})
		r.NoError(conf.Init())
		r.Zero(conf.Storages["one"].HttpTimeout,
			"a limit nobody asked for was invented for this storage")
	})

	t.Run("What is configured is kept", func(t *testing.T) {
		r := require.New(t)

		conf := storageConf(t, Storage{HttpTimeout: 90 * time.Second})
		r.NoError(conf.Init())
		r.Equal(90*time.Second, conf.Storages["one"].HttpTimeout)
	})
}

func TestStorageResponseHeaderTimeout(t *testing.T) {
	// This one may have a default: it counts neither sending nor receiving.
	t.Run("Unset is the default", func(t *testing.T) {
		r := require.New(t)

		conf := storageConf(t, Storage{})
		r.NoError(conf.Init())
		r.Equal(2*time.Minute, conf.Storages["one"].ResponseHeaderTimeout)
	})

	t.Run("What is configured is kept", func(t *testing.T) {
		r := require.New(t)

		conf := storageConf(t, Storage{ResponseHeaderTimeout: 30 * time.Second})
		r.NoError(conf.Init())
		r.Equal(30*time.Second, conf.Storages["one"].ResponseHeaderTimeout)
	})

	// Standalone mode has worker and proxy each validate a copy of an already
	// validated config.
	t.Run("A second Init keeps what the first made of it", func(t *testing.T) {
		for _, configured := range []time.Duration{0, 30 * time.Second, -1} {
			r := require.New(t)

			conf := storageConf(t, Storage{ResponseHeaderTimeout: configured})
			r.NoError(conf.Init())
			once := conf.Storages["one"].ResponseHeaderTimeout
			r.NoError(conf.Init())
			r.Equal(once, conf.Storages["one"].ResponseHeaderTimeout,
				"configured %v changed on a second Init", configured)
		}
	})
}
