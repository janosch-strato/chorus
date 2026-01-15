/*
 * Copyright © 2025 Strato GmbH
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
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/rs/zerolog"
)

func resolveAccessKeys(ctx context.Context, conf *Config, logger zerolog.Logger) error {
	if conf.Storage.Keyserver.Endpoint == "" {
		logger.Info().Msg("no keyserver configured - can not fetch acccess keys")
		return nil
	}
	if conf.Storage.Keyserver.Token == "" {
		return fmt.Errorf("KeyToken missing")
	}
	if conf.Storage.Keyserver.DecryptionKey == "" {
		return fmt.Errorf("KeyDecryptionKey missing")
	}
	decodedKey, _ := pem.Decode([]byte(conf.Storage.Keyserver.DecryptionKey))
	if decodedKey == nil || decodedKey.Type != "PRIVATE KEY" {
		return fmt.Errorf("invalid resultDecryptionKey: PEM decode failed")
	}
	decryptionKeyAny, err := x509.ParsePKCS8PrivateKey(decodedKey.Bytes)
	if err != nil {
		return fmt.Errorf("invalid resultDecryptionKey: ParsePKCS8PrivateKey failed (%w)", err)
	}
	decryptionKey, ok := decryptionKeyAny.(*rsa.PrivateKey)
	if !ok {
		return fmt.Errorf("invalid resultDecryptionKey: not a rsa private key")
	}
	timeout, err := time.ParseDuration(conf.Storage.Keyserver.Timeout)
	if err != nil {
		return fmt.Errorf("parsing timeout failed: %w", err)
	}
	if conf.Storage.Keyserver.EndpointSkipVerify {
		http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	for name, storage := range conf.Storage.Storages {
		for user, creds := range storage.Credentials {
			if creds.SecretAccessKey != "<dummy>" {
				continue
			}
			url, err := url.Parse(conf.Storage.Keyserver.Endpoint)
			if err != nil {
				return fmt.Errorf("parsing key endpoint failed: %w", err)
			}
			q := url.Query()
			q.Set("filter.accesskey", creds.AccessKeyID)
			url.RawQuery = q.Encode()
			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			req, err := http.NewRequestWithContext(reqCtx, "GET", url.String(), nil)
			if err != nil {
				return fmt.Errorf("new request failed: %w", err)
			}
			req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", conf.Storage.Keyserver.Token))
			req.Header.Add("Content-Type", "application/json")
			rsp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("http request failed: %w", err)
			}
			defer func() {
				rsp.Body.Close()
			}()
			if rsp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected http status code %d for key %s", rsp.StatusCode, creds.AccessKeyID)
			}
			var jsonRsp any
			dec := json.NewDecoder(rsp.Body)
			err = dec.Decode(&jsonRsp)
			if err != nil {
				return fmt.Errorf("decoding json body failed: %w", err)
			}
			jsonObj, ok := jsonRsp.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected response: not a json object")
			}
			items, ok := jsonObj["items"]
			if !ok {
				return fmt.Errorf("unexpected response: items missing")
			}
			itemList, ok := items.([]any)
			if !ok {
				return fmt.Errorf("unexpected response: items is not an array")
			}
			if len(itemList) != 1 {
				return fmt.Errorf("unexpected response: found %d items, expected 1", len(itemList))
			}
			item, ok := itemList[0].(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected response: item is not a json object")
			}
			properties, ok := item["properties"]
			if !ok {
				return fmt.Errorf("unexpected response: properties missing")
			}
			propertiesObj, ok := properties.(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected response: properties is not a json object")
			}
			encryptedSecretKey, ok := propertiesObj["secretKey"]
			if !ok {
				return fmt.Errorf("unexpected response: secret key missing")
			}
			encryptedSecretKeyBase64, ok := encryptedSecretKey.(string)
			if !ok {
				return fmt.Errorf("unexpected response: secret key is not a string")
			}
			encryptedSecretKeyDecoded, err := base64.StdEncoding.DecodeString(encryptedSecretKeyBase64)
			if err != nil {
				return fmt.Errorf("decode base64 failed: %w", err)
			}
			decryptedSecretKey, err := rsa.DecryptOAEP(sha256.New(), nil, decryptionKey, encryptedSecretKeyDecoded, nil)
			if err != nil {
				return fmt.Errorf("decrypt secret key failed: %w", err)
			}
			creds.SecretAccessKey = string(decryptedSecretKey)
			storage.Credentials[user] = creds
		}
		conf.Storage.Storages[name] = storage
	}
	return nil
}
