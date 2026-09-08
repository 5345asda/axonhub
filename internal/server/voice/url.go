package voice

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

func buildNativeUpstreamURL(base, protocolPath, rawQuery string) (*url.URL, error) {
	parsedBase, err := url.Parse(strings.TrimSpace(base))
	if err != nil {
		return nil, fmt.Errorf("invalid native voice upstream base URL: %w", err)
	}
	if parsedBase.Scheme == "" || parsedBase.Host == "" {
		return nil, fmt.Errorf("invalid native voice upstream base URL: %s", base)
	}

	result := *parsedBase
	result.User = nil
	result.RawQuery = filterNativeVoiceRawQuery(rawQuery)
	result.ForceQuery = false
	result.Fragment = ""
	result.RawPath = ""
	result.Path = joinNativeUpstreamPath(parsedBase.Path, protocolPath)
	return &result, nil
}

var nativeVoiceCredentialQueryNames = map[string]struct{}{
	"access-token":        {},
	"access_token":        {},
	"accesstoken":         {},
	"api-access-key":      {},
	"api-app-key":         {},
	"api-key":             {},
	"api-resource-id":     {},
	"api_access_key":      {},
	"api_app_key":         {},
	"api_key":             {},
	"api_resource_id":     {},
	"apikey":              {},
	"authorization":       {},
	"cookie":              {},
	"proxy-authorization": {},
	"resource-id":         {},
	"resource_id":         {},
	"set-cookie":          {},
	"token":               {},
	"x-api-access-key":    {},
	"x-api-app-key":       {},
	"x-api-key":           {},
	"x-api-secret":        {},
	"x-api-token":         {},
	"x-api-connect-id":    {},
	"x-api-resource-id":   {},
	"x-goog-api-key":      {},
	"x-google-api-key":    {},
}

// filterNativeVoiceRawQuery keeps provider request parameters intact while
// removing known API-key credential names that must not cross the AxonHub2 boundary.
// The raw segments are retained so ordinary query encoding and ordering are
// not rewritten as a side effect of relaying.
func filterNativeVoiceRawQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}

	parts := strings.Split(rawQuery, "&")
	kept := parts[:0]
	for _, part := range parts {
		key := part
		if index := strings.IndexByte(key, '='); index >= 0 {
			key = key[:index]
		}
		decoded, err := url.QueryUnescape(key)
		if err == nil {
			if _, credential := nativeVoiceCredentialQueryNames[strings.ToLower(strings.TrimSpace(decoded))]; credential {
				continue
			}
		}
		kept = append(kept, part)
	}

	return strings.Join(kept, "&")
}

func joinNativeUpstreamPath(basePath, protocolPath string) string {
	basePath = normalizeNativePath(basePath)
	protocolPath = normalizeNativePath(protocolPath)
	if protocolPath == "" {
		if basePath == "" {
			return "/"
		}
		return basePath
	}

	if basePath == "" || protocolPath == basePath || strings.HasPrefix(protocolPath, basePath+"/") {
		return protocolPath
	}

	return path.Join(basePath, strings.TrimPrefix(protocolPath, "/"))
}

func normalizeNativePath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "/" {
		return ""
	}
	return "/" + strings.Trim(raw, "/")
}
