/*
 * Copyright © 2024 Clyso GmbH
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

package test

import (
	"bytes"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/clyso/chorus/test/env"
	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

func TestApi_Object_CRUD(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	bucket := "object-crud"
	r := require.New(t)

	err := e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = e.MainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

	objName := "obj-crud"
	_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	source := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)

	putInfo, err := e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

	obj, err = e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	obj, err = e.F1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	obj, err = e.F2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	updated := bytes.Repeat([]byte("3"), rand.Intn(1<<20)+32*1024)
	r.NotEqualValues(source, updated)

	putInfo, err = e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(updated), int64(len(updated)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err = e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err = io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(updated, objBytes)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		obj, err = e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		obj, err = e.F1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		obj, err = e.F2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
		if err != nil {
			return false
		}
		objBytes, err = io.ReadAll(obj)
		if err != nil {
			return false
		}
		if !bytes.Equal(updated, objBytes) {
			return false
		}

		return true
	}, e.WaitShort, e.RetryShort)

	err = e.ProxyClient.RemoveObject(tstCtx, bucket, objName, mclient.RemoveObjectOptions{})
	r.NoError(err)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)

}

func TestApi_Object_Folder(t *testing.T) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	tstCtx := t.Context()
	bucket := "object-folder"
	r := require.New(t)

	err := e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = e.MainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong)

	objName := "folder/"
	_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)
	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.Error(err)

	source := []byte{}

	putInfo, err := e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err)

	objBytes, err := io.ReadAll(obj)
	r.NoError(err)
	r.EqualValues(source, objBytes)

	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err)

	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, e.WaitShort, e.RetryShort)
}

// testCriticalObjectName is a helper function for testing critical object names
func testCriticalObjectName(t *testing.T, objName, description string) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	t.Parallel()
	tstCtx := t.Context()
	bucket := "object-critical-" + strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(t.Name(), "TestApi_Object_", ""), "_", "-"))
	r := require.New(t)

	err := e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = e.MainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong)

	// Generate random test data
	source := bytes.Repeat([]byte("test-data"), rand.Intn(100)+10)

	// Test object creation
	putInfo, err := e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err, "Failed to create object with name: %s (%s)", objName, description)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	// Test object retrieval
	obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err, "Failed to retrieve object with name: %s (%s)", objName, description)
	objBytes, err := io.ReadAll(obj)
	r.NoError(err, "Failed to read object data: %s (%s)", objName, description)
	r.EqualValues(source, objBytes, "Object data mismatch for: %s (%s)", objName, description)

	// Test object stat
	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err, "Failed to stat object: %s (%s)", objName, description)
	_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err, "Failed to stat object: %s (%s)", objName, description)

	// Verify replication to all backends
	r.Eventually(func() bool {
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong, "Object not replicated to all backends: %s (%s)", objName, description)

	// Verify data consistency across all backends
	mainObj, err := e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err, "Failed to get object from main backend: %s", objName)
	mainBytes, err := io.ReadAll(mainObj)
	r.NoError(err)
	r.EqualValues(source, mainBytes, "Data mismatch in main backend for: %s", objName)

	f1Obj, err := e.F1Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err, "Failed to get object from f1 backend: %s", objName)
	f1Bytes, err := io.ReadAll(f1Obj)
	r.NoError(err)
	r.EqualValues(source, f1Bytes, "Data mismatch in f1 backend for: %s", objName)

	f2Obj, err := e.F2Client.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err, "Failed to get object from f2 backend: %s", objName)
	f2Bytes, err := io.ReadAll(f2Obj)
	r.NoError(err)
	r.EqualValues(source, f2Bytes, "Data mismatch in f2 backend for: %s", objName)

	// Test object deletion
	err = e.ProxyClient.RemoveObject(tstCtx, bucket, objName, mclient.RemoveObjectOptions{})
	r.NoError(err, "Failed to delete object: %s (%s)", objName, description)

	// Verify deletion propagated (with longer timeout)
	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong, "Object deletion not propagated: %s (%s)", objName, description)
}

// testCriticalObjectNameNotSynced is a helper function for testing critical object names which are correctly
// handled by the chorus proxy, but not handled properly by rclone.
func testCriticalObjectNameNotSynced(t *testing.T, objName, description string) {
	e := env.SetupEmbedded(t, workerConf, proxyConf)
	t.Parallel()
	tstCtx := t.Context()
	bucket := "object-critical-" + strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(t.Name(), "TestApi_Object_", ""), "_", "-"))
	r := require.New(t)

	err := e.ProxyClient.MakeBucket(tstCtx, bucket, mclient.MakeBucketOptions{Region: "us-east"})
	r.NoError(err)
	ok, err := e.ProxyClient.BucketExists(tstCtx, bucket)
	r.NoError(err)
	r.True(ok)

	r.Eventually(func() bool {
		ok, err = e.MainClient.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F1Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		ok, err = e.F2Client.BucketExists(tstCtx, bucket)
		if err != nil || !ok {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong)

	// Generate random test data
	source := bytes.Repeat([]byte("test-data"), rand.Intn(100)+10)

	// Test object creation
	putInfo, err := e.ProxyClient.PutObject(tstCtx, bucket, objName, bytes.NewReader(source), int64(len(source)), mclient.PutObjectOptions{
		ContentType: "binary/octet-stream", DisableContentSha256: true,
	})
	r.NoError(err, "Failed to create object with name: %s (%s)", objName, description)
	r.EqualValues(objName, putInfo.Key)
	r.EqualValues(bucket, putInfo.Bucket)

	// Test object retrieval
	obj, err := e.ProxyClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err, "Failed to retrieve object with name: %s (%s) from proxy", objName, description)
	objBytes, err := io.ReadAll(obj)
	r.NoError(err, "Failed to read object data: %s (%s)", objName, description)
	r.EqualValues(source, objBytes, "Object data mismatch for: %s (%s) from proxy", objName, description)
	obj, err = e.MainClient.GetObject(tstCtx, bucket, objName, mclient.GetObjectOptions{})
	r.NoError(err, "Failed to retrieve object with name: %s (%s) from main", objName, description)
	objBytes, err = io.ReadAll(obj)
	r.NoError(err, "Failed to read object data: %s (%s)", objName, description)
	r.EqualValues(source, objBytes, "Object data mismatch for: %s (%s) from main", objName, description)

	// Test object stat
	_, err = e.ProxyClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err, "Failed to stat object: %s (%s) from proxy", objName, description)
	_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
	r.NoError(err, "Failed to stat object: %s (%s) from main", objName, description)

	// Object is currently not replicated
	r.Never(func() bool {
		_, err = e.F1Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		_, err = e.F2Client.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err != nil {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong, "Object not replicated to any backends: %s (%s)", objName, description)

	// Test object deletion
	err = e.ProxyClient.RemoveObject(tstCtx, bucket, objName, mclient.RemoveObjectOptions{})
	r.NoError(err, "Failed to delete object: %s (%s)", objName, description)

	// Verify deletion propagated (with longer timeout)
	r.Eventually(func() bool {
		_, err = e.MainClient.StatObject(tstCtx, bucket, objName, mclient.StatObjectOptions{})
		if err == nil {
			return false
		}
		return true
	}, e.WaitLong, e.RetryLong, "Object deletion not propagated: %s (%s)", objName, description)
}

// Tests for critical object names that would be problematic as file system paths
// but should be valid as S3 object keys

func TestApi_Object_LeadingSlash(t *testing.T) {
	testCriticalObjectName(t, "/root/system", "Absolute path with leading slash")
}

func TestApi_Object_ControlChars(t *testing.T) {
	testCriticalObjectName(t, "file\x01\x02\x03.txt", "Control characters in name")
}
func TestApi_Object_Utf8(t *testing.T) {
	testCriticalObjectName(t, "äöüß", "UTF8")
}

func TestApi_Object_UnicodeControl(t *testing.T) {
	testCriticalObjectName(t, "file\u200B\u2060name", "Unicode control characters")
}

func TestApi_Object_DoubleDotFile(t *testing.T) {
	testCriticalObjectName(t, "..hidden", "File starting with double dots")
}

func TestApi_Object_SingleDotFile(t *testing.T) {
	testCriticalObjectName(t, ".hidden", "File starting with single dot")
}

func TestApi_Object_SpacesAndSpecial(t *testing.T) {
	testCriticalObjectName(t, " file with spaces and special chars !@#$%^&*()+={}[]|:;\"'<>,./`~", "Spaces and special characters")
}

func TestApi_Object_VeryLongName(t *testing.T) {
	testCriticalObjectName(t, "very_long_object_name_that_exceeds_typical_filesystem_limits_"+string(bytes.Repeat([]byte("x"), 200)), "Very long object name")
}

func TestApi_Object_PipeChar(t *testing.T) {
	testCriticalObjectName(t, "file|name", "Pipe character in name")
}

func TestApi_Object_QuestionMark(t *testing.T) {
	testCriticalObjectName(t, "file?name", "Question mark in name")
}

func TestApi_Object_Asterisk(t *testing.T) {
	testCriticalObjectName(t, "file*name", "Asterisk wildcard in name")
}

func TestApi_Object_MultipleSlashes(t *testing.T) {
	testCriticalObjectName(t, "path//with///multiple////slashes", "Multiple consecutive slashes")
}

func TestApi_Object_SingleTrailingSlashObject(t *testing.T) {
	testCriticalObjectName(t, "a/", "Object name ending with single slash (directory-like)")
}

func TestApi_Object_MultipleTrailingSlashes(t *testing.T) {
	testCriticalObjectName(t, "folder////", "Object name ending with multiple slashes")
}

func TestApi_Object_ColonInName(t *testing.T) {
	testCriticalObjectName(t, "file:with:colons", "Colons in filename")
}

func TestApi_Object_AngleBrackets(t *testing.T) {
	testCriticalObjectName(t, "file<with>angle<brackets>", "Angle brackets in filename")
}

func TestApi_Object_QuotesInName(t *testing.T) {
	testCriticalObjectName(t, "file\"with'quotes", "Quotes in filename")
}

func TestApi_Object_TabAndNewline(t *testing.T) {
	testCriticalObjectName(t, "file\twith\nnewline", "Tab and newline characters")
}

func TestApi_Object_SingleSlashOnly(t *testing.T) {
	testCriticalObjectName(t, "/", "Object name consisting of a single slash")
}

func TestApi_Object_OnlyMultipleSlashes(t *testing.T) {
	testCriticalObjectName(t, "////", "Object name consisting only of multiple slashes")
}

// The following tests are handled separately because the proxy handles these objecte correctly,
// but the replication backend (rclone) does not.
// Fixing this would either need a fix in rclone, or the switch to a different (possibly newly created)
// replication backend. The correct proxy bevaviour is nevertheless tested here.
func TestApi_Object_SingleDot(t *testing.T) {
	testCriticalObjectNameNotSynced(t, ".", "Object name consisting only of a single dot")
}

func TestApi_Object_DotSlash(t *testing.T) {
	testCriticalObjectNameNotSynced(t, "./", "Object name consisting only of a single dot and a slash")
}

func TestApi_Object_DotSlashDot(t *testing.T) {
	testCriticalObjectNameNotSynced(t, "./.", "Object name consisting of two dots, separatd by a slash")
}

func TestApi_Object_DotsAndSlashes(t *testing.T) {
	testCriticalObjectNameNotSynced(t, "././././", "Object name consisting of several dots and slashes")
}

func TestApi_Object_DotsSlashWord(t *testing.T) {
	testCriticalObjectNameNotSynced(t, "./name", "Object name consisting of a dot, a slash and a word")
}

func TestApi_Object_WordDoubleDot(t *testing.T) {
	testCriticalObjectNameNotSynced(t, "word/..", "Object name consisting of a word, a slash and two dots")
}

func TestApi_Object_WordDoubleDotWord(t *testing.T) {
	testCriticalObjectNameNotSynced(t, "word/../another", "Object name consisting of a word, a slash, two dots, a slash and a word")
}
