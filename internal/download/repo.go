package download

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/PrzemekMalkowski/mlaunch-go/internal/platform"
)

// RepoEntry is one row of the version listing.
type RepoEntry struct {
	Version    string // "8.0.26" or "8.0.26-11" (Percona)
	Dist       Distribution
	Stable     bool // production release (MongoDB only; Percona tags are releases)
	HostBuild  bool // a tarball exists for the host's arch/distro variant
	Downloaded bool // tarball or extracted dir present in the download dir
}

// mongoFullJSON mirrors the parts of downloads.mongodb.org/full.json we need.
type mongoFullJSON struct {
	Versions []struct {
		Version           string `json:"version"`
		ProductionRelease bool   `json:"production_release"`
		Downloads         []struct {
			Edition string `json:"edition"`
			Target  string `json:"target"`
			Arch    string `json:"arch"`
		} `json:"downloads"`
	} `json:"versions"`
}

// ListRemoteMongoDB fetches the official MongoDB release catalog and returns
// community versions, marking whether a tarball exists for the host.
func ListRemoteMongoDB(host *platform.Info, major string) ([]RepoEntry, error) {
	resp, err := httpClient.Get("https://downloads.mongodb.org/full.json")
	if err != nil {
		return nil, fmt.Errorf("fetch MongoDB release catalog: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch MongoDB release catalog: %s", resp.Status)
	}

	var full mongoFullJSON
	if err := json.NewDecoder(resp.Body).Decode(&full); err != nil {
		return nil, fmt.Errorf("parse MongoDB release catalog: %w", err)
	}

	variants := map[string]bool{}
	if cands, err := host.MongoDBVariantCandidates(); err == nil {
		for _, v := range cands {
			variants[v] = true
		}
	}

	var out []RepoEntry
	for _, v := range full.Versions {
		if !matchMajor(v.Version, major) {
			continue
		}
		e := RepoEntry{Version: v.Version, Dist: DistMongoDB, Stable: v.ProductionRelease}
		for _, d := range v.Downloads {
			// Community distro-specific tarballs have edition "targeted".
			if d.Edition == "targeted" && d.Arch == host.Arch && variants[d.Target] {
				e.HostBuild = true
				break
			}
		}
		out = append(out, e)
	}
	sortEntries(out)
	return out, nil
}

var perconaTagRe = regexp.MustCompile(`\d+\.\d+\.\d+-\d+`)

// ListRemotePercona lists Percona Server for MongoDB versions from the
// project's GitHub release tags (e.g. "psmdb-8.0.4-1"), which conveniently
// include the Percona build suffix. downloads.percona.com itself does not
// offer directory listings.
func ListRemotePercona(major string) ([]RepoEntry, error) {
	seen := map[string]bool{}
	var out []RepoEntry

	for page := 1; page <= 3; page++ {
		url := fmt.Sprintf("https://api.github.com/repos/percona/percona-server-mongodb/tags?per_page=100&page=%d", page)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch Percona tags: %w", err)
		}
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if rerr != nil {
			return nil, rerr
		}
		if resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("GitHub API rate limit reached while listing Percona tags; try again later")
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("fetch Percona tags: %s", resp.Status)
		}

		var tags []struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(body, &tags); err != nil {
			return nil, fmt.Errorf("parse Percona tags: %w", err)
		}
		if len(tags) == 0 {
			break
		}
		for _, t := range tags {
			ver := perconaTagRe.FindString(t.Name)
			if ver == "" || seen[ver] || !matchMajor(ver, major) {
				continue
			}
			seen[ver] = true
			out = append(out, RepoEntry{Version: ver, Dist: DistPercona, Stable: true})
		}
	}
	sortEntries(out)
	return out, nil
}

var searchTagRe = regexp.MustCompile(`\d+\.\d+\.\d+(?:-\d+)?`)

// ListRemoteSearch lists Percona Search for MongoDB (mongot) versions from
// the project's GitHub release tags. Every release is published as a
// tarball for both supported architectures, so HostBuild is always true.
func ListRemoteSearch(major string) ([]RepoEntry, error) {
	seen := map[string]bool{}
	var out []RepoEntry

	for page := 1; page <= 3; page++ {
		url := fmt.Sprintf("https://api.github.com/repos/percona/percona-mongot/tags?per_page=100&page=%d", page)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch Percona Search tags: %w", err)
		}
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if rerr != nil {
			return nil, rerr
		}
		if resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("GitHub API rate limit reached while listing Percona Search tags; try again later")
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("fetch Percona Search tags: %s", resp.Status)
		}

		var tags []struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(body, &tags); err != nil {
			return nil, fmt.Errorf("parse Percona Search tags: %w", err)
		}
		if len(tags) == 0 {
			break
		}
		for _, t := range tags {
			ver := searchTagRe.FindString(t.Name)
			if ver == "" || seen[ver] || !matchMajor(ver, major) {
				continue
			}
			seen[ver] = true
			out = append(out, RepoEntry{Version: ver, Dist: DistSearch, Stable: true, HostBuild: true})
		}
	}
	sortEntries(out)
	return out, nil
}

var (
	localMongoRe   = regexp.MustCompile(`^mongodb-linux-[\w-]+-(\d+\.\d+\.\d+)(?:\.tgz)?$`)
	localPerconaRe = regexp.MustCompile(`^percona-server-mongodb-(\d+\.\d+\.\d+-\d+)-.*?(?:\.tar\.gz)?$`)
	// Both the tarball (percona-search-mongodb-1.70.3-linux_x86_64.tar.gz)
	// and the versioned extraction directory (percona-search-mongodb-1.70.3).
	localSearchRe = regexp.MustCompile(`^percona-search-mongodb-(\d+\.\d+\.\d+)(?:-linux_\w+\.tar\.gz)?$`)
)

// LocalKey builds the key used by ListLocal. Percona Search tarballs are
// published without the package build suffix ("1.70.3-1" ships as
// "1.70.3"), so search versions are keyed on their base version.
func LocalKey(dist Distribution, version string) string {
	if dist == DistSearch {
		base, _, _ := strings.Cut(version, "-")
		return string(dist) + ":" + base
	}
	return string(dist) + ":" + version
}

// ListLocal scans the download directory for already downloaded tarballs
// and extracted binary directories. Keys: "mongodb:8.0.26", "percona:8.0.26-11".
func ListLocal(downloadDir string) (map[string]bool, error) {
	out := map[string]bool{}
	entries, err := os.ReadDir(downloadDir)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if m := localMongoRe.FindStringSubmatch(name); m != nil {
			out[LocalKey(DistMongoDB, m[1])] = true
		} else if m := localPerconaRe.FindStringSubmatch(name); m != nil {
			out[LocalKey(DistPercona, m[1])] = true
		} else if m := localSearchRe.FindStringSubmatch(name); m != nil {
			out[LocalKey(DistSearch, m[1])] = true
		}
	}
	return out, nil
}

// matchMajor reports whether version matches the major filter: "8" matches
// 8.x.y, "8.0" matches 8.0.y, "" matches everything.
func matchMajor(version, major string) bool {
	if major == "" {
		return true
	}
	return version == major || strings.HasPrefix(version, major+".")
}

// sortEntries sorts newest-first by numeric version components.
func sortEntries(entries []RepoEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return versionLess(entries[j].Version, entries[i].Version)
	})
}

var numRe = regexp.MustCompile(`\d+`)

// versionLess compares versions by their numeric components ("8.0.26-11").
func versionLess(a, b string) bool {
	as := numRe.FindAllString(a, -1)
	bs := numRe.FindAllString(b, -1)
	for i := 0; i < len(as) && i < len(bs); i++ {
		ai, _ := strconv.Atoi(as[i])
		bi, _ := strconv.Atoi(bs[i])
		if ai != bi {
			return ai < bi
		}
	}
	return len(as) < len(bs)
}
