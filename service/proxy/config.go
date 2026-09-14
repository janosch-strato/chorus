/*
 * Copyright © 2024 Clyso GmbH
 * Copyright © 2025 STRATO GmbH
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

package proxy

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"time"

	"github.com/clyso/chorus/pkg/config"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/service/proxy/auth"
	"github.com/clyso/chorus/service/proxy/cors"
)

//go:embed config.yaml
var configFile embed.FS

func defaultConfig() fs.File {
	defaultFile, err := configFile.Open("config.yaml")
	if err != nil {
		panic(err)
	}
	return defaultFile
}

type Config struct {
	config.Common `yaml:",inline,omitempty" mapstructure:",squash"`

	Enabled bool              `yaml:"enabled"`
	Storage *s3.StorageConfig `yaml:"storage,omitempty"`
	Auth    *auth.Config      `yaml:"auth,omitempty"`
	Port    int               `yaml:"port"`
	Address s3.ConfAddr       `yaml:"address"`
	Cors    *cors.Config      `yaml:"cors"`

	// ReadFromDestination serves object reads from the replication destination
	// for objects the migration has already copied. Meant for migrations away
	// from a storage that is too slow to read from.
	ReadFromDestination bool `yaml:"readFromDestination"`
	// How long such a read waits for the destination to begin answering,
	// connecting included. Negative turns it off.
	DestinationAnswerTimeout time.Duration `yaml:"destinationAnswerTimeout"`
	// Answer timeouts in a row before such reads are paused. Negative turns
	// the pausing off.
	DestinationTimeoutLimit int `yaml:"destinationTimeoutLimit"`
	// How long reads stay on the source once that limit is reached.
	DestinationPause time.Duration `yaml:"destinationPause"`

	// What this proxy calls itself in meta.DeleteIntent, and whose deletes it
	// finishes on starting up. Has to survive a restart and differ from every
	// other proxy. Unset means the hostname.
	InstanceID string `yaml:"instanceID"`
}

const defaultDestinationAnswerTimeout = time.Second * 5

const (
	defaultDestinationTimeoutLimit = 3
	defaultDestinationPause        = time.Minute * 5
)

func (c *Config) Validate() error {
	if err := c.Common.Validate(); err != nil {
		return err
	}
	if c.Storage == nil {
		return fmt.Errorf("app config: empty storages config")
	}
	if err := c.Storage.Init(); err != nil {
		return err
	}
	if c.Auth == nil {
		return fmt.Errorf("proxy config: empty Auth config")
	}
	if c.Auth.UseStorage != "" {
		if _, ok := c.Storage.Storages[c.Auth.UseStorage]; !ok {
			return fmt.Errorf("proxy config: auth UseStorage points to unknown storage")
		}
	}
	if len(c.Auth.Custom) != 0 {
		var storCreds map[string]s3.CredentialsV4
		for _, storage := range c.Storage.Storages {
			storCreds = storage.Credentials
			break
		}
		for user := range c.Auth.Custom {
			if _, ok := storCreds[user]; !ok {
				return fmt.Errorf("proxy config: auth custom credentials unknown user %q", user)
			}
		}
	}
	if c.Auth.UseStorage == "" && len(c.Auth.Custom) == 0 {
		return fmt.Errorf("proxy config: auth credentials enabled but not set")
	}

	if c.DestinationAnswerTimeout == 0 {
		c.DestinationAnswerTimeout = defaultDestinationAnswerTimeout
	}
	if c.DestinationTimeoutLimit == 0 {
		c.DestinationTimeoutLimit = defaultDestinationTimeoutLimit
	}
	if c.DestinationPause == 0 {
		c.DestinationPause = defaultDestinationPause
	}

	if c.InstanceID == "" {
		host, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("proxy config: unable to name this instance from the hostname, set instanceID: %w", err)
		}
		c.InstanceID = host
	}

	if c.Port <= 0 {
		return fmt.Errorf("proxy config: Port must be positive: %d", c.Port)
	}

	return nil
}

func GetConfig(src ...config.Src) (*Config, error) {
	dc := defaultConfig()
	var conf Config
	cfgSource := []config.Src{config.Reader(dc, "proxy_default_cfg")}
	cfgSource = append(cfgSource, src...)
	err := config.Get(&conf, cfgSource...)
	_ = dc.Close()
	if err != nil {
		return nil, err
	}
	return &conf, err
}
