package voice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildNativeUpstreamURLDeduplicatesRegisteredBasePath(t *testing.T) {
	upstream, err := buildNativeUpstreamURL("https://provider.test/v1", "/v1/t2a_v2", "model=speech-2.8-hd")

	require.NoError(t, err)
	require.Equal(t, "https://provider.test/v1/t2a_v2?model=speech-2.8-hd", upstream.String())
}

func TestBuildNativeUpstreamURLPreservesCustomPrefixAndStripsUserinfo(t *testing.T) {
	upstream, err := buildNativeUpstreamURL("https://user:secret@provider.test/prefix", "/v1/t2a_v2", "")

	require.NoError(t, err)
	require.Equal(t, "https://provider.test/prefix/v1/t2a_v2", upstream.String())
}

func TestBuildNativeUpstreamURLStripsCredentialQueryParameters(t *testing.T) {
	upstream, err := buildNativeUpstreamURL(
		"https://provider.test",
		"/v1/t2a_v2",
		"model=speech-2.8-hd&api_key=downstream-secret&voice=best&token=also-secret&api_app_key=legacy-app&api_access_key=legacy-access&api_resource_id=legacy-resource&resource_id=route-resource&x-api-app-key=legacy-app-2&x-api-access-key=legacy-access-2&x-api-token=legacy-token&x-api-connect-id=legacy-connect&x-api-resource-id=legacy-resource-2&x-goog-api-key=legacy-google&x-google-api-key=legacy-google-2",
	)

	require.NoError(t, err)
	require.Equal(t, "https://provider.test/v1/t2a_v2?model=speech-2.8-hd&voice=best", upstream.String())
}
