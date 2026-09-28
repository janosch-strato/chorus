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

package features

import (
	"context"
	"sync/atomic"
)

type Config struct {
	Versioning        bool `yaml:"versioning"`
	Tagging           bool `yaml:"tagging"`
	ACL               bool `yaml:"acl"`
	Lifecycle         bool `yaml:"lifecycle"`
	Policy            bool `yaml:"policy"`
	PreserveACLGrants bool `yaml:"preserveACLGrants"`
}

// Held atomically because standalone mode starts worker and proxy as
// goroutines and each sets the features on the way up, while every feature
// check reads them.
var val atomic.Pointer[Config]

func Set(c *Config) {
	val.Store(c)
}

func Versioning(_ context.Context) bool {
	return val.Load().Versioning
}

func Tagging(_ context.Context) bool {
	return val.Load().Tagging
}

func ACL(_ context.Context) bool {
	return val.Load().ACL
}

func Lifecycle(_ context.Context) bool {
	return val.Load().Lifecycle
}

func Policy(_ context.Context) bool {
	return val.Load().Policy
}

func PreserveACLGrants(_ context.Context) bool {
	return val.Load().PreserveACLGrants
}
