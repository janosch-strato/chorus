package s3client

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/s3"
)

func TestIsReadOnly(t *testing.T) {
	t.Run("Reads follow the client", func(t *testing.T) {
		r := require.New(t)

		r.True(isReadOnly(s3.GetObject))
		r.True(isReadOnly(s3.HeadObject))
		r.True(isReadOnly(s3.ListObjectsV2))
		r.True(isReadOnly(s3.ListParts))
		r.True(isReadOnly(s3.SelectObjectContent))
	})

	t.Run("Writes do not", func(t *testing.T) {
		r := require.New(t)

		r.False(isReadOnly(s3.PutObject))
		r.False(isReadOnly(s3.DeleteObject))
		r.False(isReadOnly(s3.DeleteObjects))
		r.False(isReadOnly(s3.CopyObject))
		r.False(isReadOnly(s3.CompleteMultipartUpload))
		r.False(isReadOnly(s3.UploadPart))
		r.False(isReadOnly(s3.UploadPartCopy))
	})

	t.Run("An unparsed method is treated as a write", func(t *testing.T) {
		require.New(t).False(isReadOnly(s3.UndefinedMethod))
	})

	// So that a method added later is classified rather than inheriting a
	// default.
	t.Run("Every method is classified by its name", func(t *testing.T) {
		r := require.New(t)

		writePrefixes := []string{"Put", "Delete", "Create", "Complete", "Abort", "Restore", "Copy", "Upload", "Write"}
		readPrefixes := []string{"Get", "Head", "List"}
		named := map[string]bool{
			"UndefinedMethod":     false,
			"SelectObjectContent": true,
		}

		for m := s3.UndefinedMethod; m <= s3.GetObjectTorrent; m++ {
			name := m.String()
			r.NotContains(name, "Method(", "method %d is outside the generated names, extend this test", m)

			if want, ok := named[name]; ok {
				r.Equal(want, isReadOnly(m), "%s", name)
				continue
			}
			switch {
			case hasAnyPrefix(name, writePrefixes):
				r.False(isReadOnly(m), "%s writes to the storage", name)
			case hasAnyPrefix(name, readPrefixes):
				r.True(isReadOnly(m), "%s only reads from the storage", name)
			default:
				r.Fail("unclassified method", "%s matches no prefix, classify it in this test", name)
			}
		}
	})
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}
