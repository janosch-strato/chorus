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

package dom

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

var (
	ErrInternal             = errors.New("InternalError")
	ErrNotImplemented       = errors.New("NotImplemented")
	ErrInvalidStorageConfig = errors.New("InvalidStorageConfig")
	ErrAlreadyExists        = errors.New("AlreadyExists")
	ErrNotFound             = errors.New("NotFound")
	ErrInvalidArg           = errors.New("InvalidArg")
	ErrAuth                 = errors.New("AuthError")
	ErrPolicy               = errors.New("PolicyError")
	ErrRoutingBlock         = errors.New("RoutingBlockedError")

	// Nothing reached the storage, so there is nothing to replicate.
	ErrClientGone = errors.New("ClientGone")
	// The request was written and no answer was read, so the change may have
	// been carried out: followed up on as if it had.
	ErrStorageAnswerLost = errors.New("StorageAnswerLost")
)

// StorageMayHaveActed reports whether the storage may have carried out the
// request, so that the change has to be followed up on.
func StorageMayHaveActed(err error) bool {
	return err == nil || errors.Is(err, ErrStorageAnswerLost)
}

func ErrCode(err error) int {
	switch {
	case errors.Is(err, ErrNotImplemented):
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

type ErrRateLimitExceeded struct {
	RetryIn time.Duration
}

func (e *ErrRateLimitExceeded) Error() string {
	return "RateLimitExceeded"
}

func ErrContains(err error, text ...string) bool {
	if err == nil {
		return false
	}
	for _, s := range text {
		if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(s)) {
			return true
		}
	}
	return false
}
