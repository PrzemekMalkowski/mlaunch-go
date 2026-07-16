// Package download resolves, fetches and extracts MongoDB / Percona Server
// for MongoDB tarballs.
package download

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PrzemekMalkowski/mlaunch-go/internal/platform"
)

// Distribution selects the upstream vendor.
type Distribution string

const (
	DistMongoDB Distribution = "mongodb"
	DistPercona Distribution = "percona"
)

// Options controls the resolution/download.
type Options struct {
	Dist    Distribution
	Version string // "8.3.4" for MongoDB, "8.0.4" or "8.0.4-1" for Percona
	// DownloadDir is where the .tgz is stored and extracted. Defaults to ".".
	DownloadDir string
	// URL, when non-empty, skips resolution entirely.
	URL string
	// Quiet suppresses progress output.
	Quiet bool
}

// Result describes a completed download.
type Result struct {
	URL     string
	Archive string // path of the downloaded tarball
	BinDir  string // extracted directory containing mongod / mongos
}

var httpClient = &http.Client{Timeout: 10 * time.Minute}

// Resolve computes the list of candidate URLs for the given options on the
// given host platform, ordered from best match to broadest fallback.
func Resolve(opts Options, host *platform.Info) ([]string, error) {
	if opts.URL != "" {
		return []string{opts.URL}, nil
	}
	if opts.Version == "" {
		return nil, fmt.Errorf("--version is required (e.g. --version 8.3.4)")
	}

	switch opts.Dist {
	case DistMongoDB, "":
		variants, err := host.MongoDBVariantCandidates()
		if err != nil {
			return nil, err
		}
		urls := make([]string, 0, len(variants))
		for _, v := range variants {
			// e.g. https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-8.3.4.tgz
			urls = append(urls, fmt.Sprintf(
				"https://fastdl.mongodb.org/linux/mongodb-linux-%s-%s-%s.tgz",
				host.Arch, v, opts.Version))
		}
		return urls, nil

	case DistPercona:
		return perconaCandidates(opts.Version, host)

	default:
		return nil, fmt.Errorf("unknown distribution %q (want %q or %q)", opts.Dist, DistMongoDB, DistPercona)
	}
}

// perconaCandidates builds URLs of the form:
//
//	https://downloads.percona.com/downloads/percona-server-mongodb-8.0/
//	  percona-server-mongodb-8.0.4-1/binary/tarball/
//	  percona-server-mongodb-8.0.4-1-x86_64.ol9-minimal.tar.gz
//
// If the user did not pass a Percona build suffix ("-1"), a small range of
// build numbers is probed.
func perconaCandidates(version string, host *platform.Info) ([]string, error) {
	flavors, err := host.PerconaFlavorCandidates()
	if err != nil {
		return nil, err
	}

	base, _, hasBuild := strings.Cut(version, "-")
	var fullVersions []string
	if hasBuild {
		fullVersions = []string{version}
	} else {
		// Percona appends a build number to the upstream version, and it is
		// not always small: e.g. 8.0.26 was released as 8.0.26-11. Probe a
		// generous range (each miss is a single cheap HEAD request). Passing
		// the full version (--version 8.0.26-11) skips probing entirely.
		for b := 1; b <= 30; b++ {
			fullVersions = append(fullVersions, fmt.Sprintf("%s-%d", base, b))
		}
	}

	parts := strings.SplitN(base, ".", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid Percona version %q, expected e.g. 8.0.4 or 8.0.4-1", version)
	}
	series := parts[0] + "." + parts[1] // "8.0"

	var urls []string
	for _, fv := range fullVersions {
		for _, flavor := range flavors {
			urls = append(urls, fmt.Sprintf(
				"https://downloads.percona.com/downloads/percona-server-mongodb-%s/percona-server-mongodb-%s/binary/tarball/percona-server-mongodb-%s-%s.%s-minimal.tar.gz",
				series, fv, fv, host.Arch, flavor))
		}
	}
	return urls, nil
}

// probe returns the first URL from candidates that exists (HTTP 200 on HEAD,
// falling back to a ranged GET for servers that reject HEAD).
func probe(candidates []string, quiet bool) (string, error) {
	var lastErr error
	for _, u := range candidates {
		ok, err := urlExists(u)
		if err != nil {
			lastErr = err
			continue
		}
		if ok {
			return u, nil
		}
		if !quiet {
			fmt.Fprintf(os.Stderr, "  not available: %s\n", u)
		}
	}
	if lastErr != nil {
		return "", fmt.Errorf("no candidate URL is reachable (last error: %w)", lastErr)
	}
	return "", fmt.Errorf("no matching tarball found among %d candidate URLs", len(candidates))
}

func urlExists(u string) (bool, error) {
	resp, err := httpClient.Head(u)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return true, nil
		}
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
			return false, nil
		}
	}
	// Fall back to a 1-byte ranged GET (some CDNs dislike HEAD).
	req, rerr := http.NewRequest(http.MethodGet, u, nil)
	if rerr != nil {
		return false, rerr
	}
	req.Header.Set("Range", "bytes=0-0")
	resp2, err2 := httpClient.Do(req)
	if err2 != nil {
		return false, err2
	}
	defer resp2.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp2.Body, 4))
	return resp2.StatusCode == http.StatusOK || resp2.StatusCode == http.StatusPartialContent, nil
}

// Fetch resolves, downloads and extracts the tarball. If the tarball or the
// extracted directory already exist locally, the corresponding step is
// skipped, making Fetch idempotent.
func Fetch(opts Options, host *platform.Info) (*Result, error) {
	dir := opts.DownloadDir
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	candidates, err := Resolve(opts, host)
	if err != nil {
		return nil, err
	}

	// If a previously downloaded archive for any candidate is present,
	// reuse it without touching the network.
	for _, u := range candidates {
		local := filepath.Join(dir, filepath.Base(u))
		if st, err := os.Stat(local); err == nil && st.Size() > 0 {
			if !opts.Quiet {
				fmt.Fprintf(os.Stderr, "reusing existing archive %s\n", local)
			}
			return extractResult(u, local, dir, opts.Quiet)
		}
	}

	var chosen string
	if len(candidates) == 1 {
		chosen = candidates[0]
	} else {
		if !opts.Quiet {
			fmt.Fprintf(os.Stderr, "resolving tarball for %s ...\n", host)
		}
		chosen, err = probe(candidates, opts.Quiet)
		if err != nil {
			return nil, err
		}
	}

	local := filepath.Join(dir, filepath.Base(chosen))
	if err := downloadFile(chosen, local, opts.Quiet); err != nil {
		return nil, err
	}
	return extractResult(chosen, local, dir, opts.Quiet)
}

func extractResult(url, archive, dir string, quiet bool) (*Result, error) {
	binDir, err := extractTarGz(archive, dir, quiet)
	if err != nil {
		return nil, err
	}
	return &Result{URL: url, Archive: archive, BinDir: binDir}, nil
}

func downloadFile(url, dest string, quiet bool) error {
	if !quiet {
		fmt.Fprintf(os.Stderr, "downloading %s\n", url)
	}
	resp, err := httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	tmp := dest + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	var src io.Reader = resp.Body
	if !quiet && resp.ContentLength > 0 {
		src = &progressReader{r: resp.Body, total: resp.ContentLength}
	}
	_, cerr := io.Copy(f, src)
	closeErr := f.Close()
	if !quiet {
		fmt.Fprintln(os.Stderr)
	}
	if cerr != nil {
		os.Remove(tmp)
		return cerr
	}
	if closeErr != nil {
		os.Remove(tmp)
		return closeErr
	}
	return os.Rename(tmp, dest)
}

type progressReader struct {
	r     io.Reader
	total int64
	done  int64
	last  time.Time
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.done += int64(n)
	if time.Since(p.last) > 500*time.Millisecond || err == io.EOF {
		p.last = time.Now()
		fmt.Fprintf(os.Stderr, "\r  %d / %d MiB (%d%%)",
			p.done>>20, p.total>>20, p.done*100/p.total)
	}
	return n, err
}

// extractTarGz unpacks archive into destDir and returns the path to the
// top-level directory of the tarball (e.g. mongodb-linux-x86_64-ubuntu2204-8.3.4).
// If that directory already exists with a bin/mongod inside, extraction is
// skipped.
func extractTarGz(archive, destDir string, quiet bool) (string, error) {
	// Work with an absolute destination path. This is required for the
	// escape check below: with a relative destDir like ".",
	// filepath.Join(".", name) strips the leading "./", so the prefix
	// comparison would wrongly flag every entry as escaping.
	absDest, err := filepath.Abs(destDir)
	if err != nil {
		return "", err
	}
	destDir = absDest

	// Guess the top-level dir from the archive name to allow skipping.
	base := filepath.Base(archive)
	base = strings.TrimSuffix(base, ".tgz")
	base = strings.TrimSuffix(base, ".tar.gz")
	guess := filepath.Join(destDir, base)
	if _, err := os.Stat(filepath.Join(guess, "bin", "mongod")); err == nil {
		if !quiet {
			fmt.Fprintf(os.Stderr, "reusing extracted binaries in %s\n", guess)
		}
		return guess, nil
	}

	f, err := os.Open(archive)
	if err != nil {
		return "", err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", fmt.Errorf("%s: %w", archive, err)
	}
	defer gz.Close()

	if !quiet {
		fmt.Fprintf(os.Stderr, "extracting %s ...\n", archive)
	}

	tr := tar.NewReader(gz)
	topLevel := ""

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		name := filepath.Clean(hdr.Name)
		if name == "." || strings.HasPrefix(name, "..") {
			continue
		}
		target := filepath.Join(destDir, name)
		if target != destDir && !strings.HasPrefix(target, destDir+string(os.PathSeparator)) {
			return "", fmt.Errorf("tar entry escapes destination: %s", hdr.Name)
		}
		if topLevel == "" {
			first := strings.SplitN(name, string(os.PathSeparator), 2)[0]
			topLevel = filepath.Join(destDir, first)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return "", err
			}
		case tar.TypeSymlink:
			os.Remove(target)
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return "", err
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return "", err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return "", err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return "", err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return "", err
			}
			if err := out.Close(); err != nil {
				return "", err
			}
		}
	}

	if topLevel == "" {
		return "", fmt.Errorf("archive %s appears to be empty", archive)
	}
	return topLevel, nil
}
