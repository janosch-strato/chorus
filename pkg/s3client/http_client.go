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

package s3client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_credentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

// Connection-pool sizing for the S3 transport used by minio-go.
// The minio-go default caps idle connections per host at ~16, which
// forces fresh TCP+TLS handshakes once tasks-in-flight to one storage
// exceeds that. Sized to comfortably cover worker.concurrency in the
// low hundreds against a single source/destination host.
const (
	s3MaxIdleConns        = 1024
	s3MaxIdleConnsPerHost = 256
)

// measuredTransport records how long a storage takes to answer the requests
// the s3 sdk clients make. Those do not go through client.Do, so without this
// the acl, tag and stat calls of the worker are invisible.
type measuredTransport struct {
	next    http.RoundTripper
	storage string
}

func (t measuredTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	inFlight := metrics.StorageInFlight(t.storage, req.Method)
	inFlight.Inc()
	start := time.Now()
	resp, err := t.next.RoundTrip(req)
	inFlight.Dec()
	metrics.StorageHTTPDuration(t.storage, req.Method, time.Since(start))
	return resp, err
}

// newS3Transport mirrors http.DefaultTransport apart from the idle
// connection pool, which is sized for high-concurrency workers.
func newS3Transport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          s3MaxIdleConns,
		MaxIdleConnsPerHost:   s3MaxIdleConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

func newClient(ctx context.Context, conf s3.Storage, name, user string, metricsSvc metrics.S3Service, _ trace.TracerProvider) (Client, error) {
	c := &client{
		// Without a transport this would use http.DefaultTransport, which
		// keeps 2 idle connections per host, so every request beyond two in
		// flight pays a fresh tcp and tls handshake. It is also shared
		// process wide, which lets an InsecureSkipVerify set for one purpose
		// reach every other user of the default transport.
		c: &http.Client{
			Timeout:   conf.HttpTimeout,
			Transport: newS3Transport(),
		},
		online:     &atomic.Bool{},
		conf:       conf,
		name:       name,
		user:       user,
		cred:       conf.Credentials[user],
		metricsSvc: metricsSvc,
	}

	mc, err := mclient.New(conf.Address.Value(), &mclient.Options{
		Creds:     credentials.NewStaticV4(c.cred.AccessKeyID, c.cred.SecretAccessKey, ""),
		Secure:    conf.IsSecure,
		Transport: measuredTransport{next: newS3Transport(), storage: name},
	})
	if err != nil {
		return nil, err
	}
	c.s3 = newMinioClient(name, user, mc, metricsSvc)
	if err = isOnline(ctx, c); err != nil {
		return nil, fmt.Errorf("s3 is offline: %w", err)
	}
	c.online.Store(true)
	go func(duration time.Duration) {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				// Do health check the first time and ONLY if the connection is marked offline
				if !c.online.Load() {
					if err = isOnline(ctx, c); err != nil {
						c.online.Store(true)
					}
				}
				timer.Reset(duration)
			}
		}
	}(conf.HealthCheckInterval)

	awsClient, err := newAWSClient(conf, name, user, metricsSvc)
	if err != nil {
		return nil, err
	}
	c.aws = awsClient
	snsEndpoint := conf.Address.GetEndpoint(conf.IsSecure)

	c.sns = sns.NewFromConfig(aws.Config{
		RetryMaxAttempts: 1,
		Region:           "default",
		Credentials:      aws_credentials.NewStaticCredentialsProvider(conf.Credentials[user].AccessKeyID, conf.Credentials[user].SecretAccessKey, ""),
		// EndpointResolver: aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		// 	return aws.Endpoint{URL: snsEndpoint}, nil
		// }),
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: snsEndpoint}, nil
		}),
	})

	return c, nil
}

func isOnline(ctx context.Context, c *client) error {
	_, err := c.s3.GetBucketLocation(ctx, "probe-health-test")
	if err == nil {
		return nil
	} else if !mclient.IsNetworkOrHostDown(err, false) {
		switch mclient.ToErrorResponse(err).Code {
		case "NoSuchBucket", "AccessDenied", "":
			return nil
		}
	}
	return err
}

type client struct {
	c          *http.Client
	s3         *S3
	aws        *AWS
	sns        *sns.Client
	online     *atomic.Bool
	conf       s3.Storage
	name       string
	user       string
	cred       s3.CredentialsV4
	metricsSvc metrics.S3Service
}

func (c *client) SNS() *sns.Client {
	return c.sns
}

func (c *client) AWS() *AWS {
	return c.aws
}

func (c *client) Name() string {
	return c.name
}

func (c *client) Config() s3.Storage {
	return c.conf
}

func (c *client) S3() *S3 {
	return c.s3
}

func (c *client) Do(req *http.Request) (resp *http.Response, isApiErr bool, err error) {
	start := time.Now()
	ctx, span := otel.Tracer("").Start(req.Context(), fmt.Sprintf("clientDo.%s", xctx.GetMethod(req.Context()).String()))
	span.SetAttributes(attribute.String("storage", c.name), attribute.String("user", c.user))
	if xctx.GetBucket(ctx) != "" {
		span.SetAttributes(attribute.String("bucket", xctx.GetBucket(ctx)))
	}
	if xctx.GetObject(ctx) != "" {
		span.SetAttributes(attribute.String("object", xctx.GetObject(ctx)))
	}
	defer span.End()
	req = req.WithContext(ctx)
	defer func() {
		if mclient.IsNetworkOrHostDown(err, false) {
			c.online.Store(false)
		}
	}()
	defer func() {
		if err != nil {
			return
		}
		method := xctx.GetMethod(req.Context())
		flow := xctx.GetFlow(req.Context())
		c.metricsSvc.Count(flow, c.name, method)
		c.metricsSvc.Duration(flow, c.name, method, time.Since(start))
		switch method {
		case s3.GetObject:
			if resp.ContentLength != 0 {
				c.metricsSvc.Download(flow, c.name, xctx.GetBucket(req.Context()), int(resp.ContentLength))
			}
		case s3.PutObject, s3.UploadPart:
			if req.ContentLength != 0 {
				c.metricsSvc.Upload(flow, c.name, xctx.GetBucket(req.Context()), int(req.ContentLength))
			}
		}
	}()

	// Parse bucket and object using the s3 package helper
	bucket, object, bucketInHostname := s3.ParseBucketAndObject(req, c.conf.Domains)

	url := req.URL
	var newReq *http.Request

	if bucketInHostname {
		url.Host = bucket + "." + c.conf.Address.Value()
	} else {
		url.Host = c.conf.Address.Value()
	}
	url.Scheme = "http"
	if c.conf.IsSecure {
		url.Scheme = "https"
	}
	url.OmitHost = false
	url.ForceQuery = false

	_, copyReqSpan := otel.Tracer("").Start(ctx, fmt.Sprintf("clientDo.%s.CopyReq", xctx.GetMethod(req.Context()).String()))
	var body io.Reader = http.NoBody
	if req.ContentLength != 0 {
		body = io.NopCloser(req.Body)
	}
	newReq, err = http.NewRequest(req.Method, url.String(), body)
	if err != nil {
		copyReqSpan.End()
		return nil, false, err
	}
	newReq.ContentLength = req.ContentLength
	newReq.Header = req.Header
	copyReqSpan.End()

	if url.Host == req.Host {
		// transparent proxy mode, forward request as-is
	} else {
		_, signReqSpan := otel.Tracer("").Start(ctx, fmt.Sprintf("clientDo.%s.SignReq", xctx.GetMethod(req.Context()).String()))
		if s3.IsRequestSignatureV4(req) { //nolint:gocritic // switch subject would be empty
			newReq = signV4(newReq, c.cred.AccessKeyID, c.cred.SecretAccessKey, "us-east-1") // todo: get location if needed ("us-east-1")
		} else if s3.IsRequestSignatureV2(req) {
			domains := make([]string, len(c.conf.Domains))
			for i, dom := range c.conf.Domains {
				domains[i] = dom.Value()
			}
			newReq, err = signV2(newReq, c.cred.AccessKeyID, c.cred.SecretAccessKey, domains)
			if err != nil {
				return nil, false, err
			}
		} else {
			// Should have been avoided by isReqAuthenticated() in the first place
			return nil, false, dom.ErrInternal
		}
		signReqSpan.End()
	}

	// Report connection level timings into the request timing collector, if
	// the caller collects them. The outgoing request keeps a background
	// context on purpose: attaching the client context would make a client
	// disconnect cancel the storage request, which it does not today.
	timing := xctx.GetTiming(ctx)
	if timing != nil {
		newReq = newReq.WithContext(xctx.TraceContext(context.Background(), timing))
	}

	inFlight := metrics.StorageInFlight(c.name, xctx.GetMethod(ctx).String())
	inFlight.Inc()
	_, doReqSpan := otel.Tracer("").Start(ctx, fmt.Sprintf("clientDo.%s.DoReq", xctx.GetMethod(req.Context()).String()))
	resp, err = c.c.Do(newReq)
	doReqSpan.End()
	inFlight.Dec()
	if resp != nil {
		timing.SetRequestID(resp.Header.Get("x-amz-request-id"))
	}
	if resp != nil && !successStatus[resp.StatusCode] {
		isApiErr = true
		// Read the body to be saved later.
		var errBodyBytes []byte
		errBodyBytes, err = io.ReadAll(resp.Body)
		// res.Body should be closed
		closeResponse(resp)

		// Save the body.
		errBodySeeker := bytes.NewReader(errBodyBytes)
		resp.Body = io.NopCloser(errBodySeeker)

		// For errors verify if its retryable otherwise fail quickly.
		err = mclient.ToErrorResponse(httpRespToErrorResponse(resp, bucket, object))

		// Save the body back again.
		_, _ = errBodySeeker.Seek(0, 0) // Seek back to starting point.
		resp.Body = io.NopCloser(errBodySeeker)
		return
	}
	if err != nil {
		return nil, false, err
	}
	return
}

func (c *client) IsOnline() bool {
	return c.online.Load()
}
