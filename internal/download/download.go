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

// Phase names the stage a Fetch is currently in; it is reported through
// Options.Progress so callers can drive a progress display.
type Phase string

const (
	PhaseProbe    Phase = "probe"    // looking for a URL that exists
	PhaseDownload Phase = "download" // transferring the tarball
	PhaseExtract  Phase = "extract"  // unpacking the tarball
	PhaseReuse    Phase = "reuse"    // found locally, nothing to do
)

// Event reports progress of a Fetch. Done/Total are bytes during
// PhaseDownload and candidate counts during PhaseProbe; Total is 0 when
// the size is unknown.
type Event struct {
	Phase Phase
	Done  int64
	Total int64
	URL   string // the URL being probed/downloaded, when relevant
	Path  string // the local archive or directory, when relevant
}

// Options controls the resolution/download.
type Options struct {
	Dist    Distribution
	Version string // "8.3.4" for MongoDB, "8.0.4" or "8.0.4-1" for Percona
	// DownloadDir is where the .tgz is stored and extracted. Defaults to ".".
	DownloadDir string
	// URL, when non-empty, skips resolution entirely.
	URL string
	// Quiet suppresses the plain-text fallback output on stderr. It has no
	// effect on Progress, which is always called when set.
	Quiet bool
	// Progress, when set, receives events as the download advances. It is
	// called from the goroutine driving Fetch, frequently during
	// PhaseDownload, and must not block for long.
	Progress func(Event)
}

// notify reports an event when the caller asked for them.
func notify(fn func(Event), ev Event) {
	if fn != nil {
		fn(ev)
	}
}

// Result describes a completed download.
type Result struct {
	URL     string
	Archive string // path of the downloaded tarball
	BinDir  string // extracted directory containing mongod / mongos
	// ArchiveCached is true when the tarball was already in the download
	// directory, so nothing was transferred.
	ArchiveCached bool
	// BinCached is true when the extracted directory was already present,
	// so nothing was unpacked. ArchiveCached && BinCached means the
	// binaries were ready to use before this call.
	BinCached bool
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
			urls = append(urls, mongoDBTarballURL(host, v, opts.Version))
		}
		return urls, nil

	case DistPercona:
		return perconaCandidates(opts.Version, host)

	case DistSearch:
		return nil, fmt.Errorf("%q is the search component, not a server distribution; launch it with 'init --percona-search' or list versions with 'show-repo --distro search'", DistSearch)

	default:
		return nil, fmt.Errorf("unknown distribution %q (want %q or %q)", opts.Dist, DistMongoDB, DistPercona)
	}
}

// mongoDBTarballURL builds a community tarball URL. The variant comes
// after the architecture on Linux and before it on macOS:
//
//	https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-8.3.4.tgz
//	https://fastdl.mongodb.org/osx/mongodb-macos-arm64-8.0.34.tgz
func mongoDBTarballURL(host *platform.Info, variant, version string) string {
	if host.OS == "macos" {
		return fmt.Sprintf("https://fastdl.mongodb.org/osx/mongodb-%s-%s-%s.tgz",
			variant, host.Arch, version)
	}
	return fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-%s-%s-%s.tgz",
		host.Arch, variant, version)
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
func probe(candidates []string, quiet bool, progress func(Event)) (string, error) {
	var lastErr error
	total := int64(len(candidates))
	for i, u := range candidates {
		notify(progress, Event{Phase: PhaseProbe, Done: int64(i + 1), Total: total, URL: u})
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
			notify(opts.Progress, Event{Phase: PhaseReuse, URL: u, Path: local, Done: st.Size(), Total: st.Size()})
			return extractResult(u, local, dir, true, opts)
		}
	}

	var chosen string
	if len(candidates) == 1 {
		chosen = candidates[0]
	} else {
		if !opts.Quiet {
			fmt.Fprintf(os.Stderr, "resolving tarball for %s ...\n", host)
		}
		chosen, err = probe(candidates, opts.Quiet, opts.Progress)
		if err != nil {
			return nil, err
		}
	}

	local := filepath.Join(dir, filepath.Base(chosen))
	if err := downloadFile(chosen, local, opts.Quiet, opts.Progress); err != nil {
		return nil, err
	}
	return extractResult(chosen, local, dir, false, opts)
}

func extractResult(url, archive, dir string, archiveCached bool, opts Options) (*Result, error) {
	binDir, reused, err := extractTarGz(archive, dir, filepath.Join("bin", "mongod"), "", opts.Quiet, opts.Progress)
	if err != nil {
		return nil, err
	}
	return &Result{
		URL:           url,
		Archive:       archive,
		BinDir:        binDir,
		ArchiveCached: archiveCached,
		BinCached:     reused,
	}, nil
}

func downloadFile(url, dest string, quiet bool, progress func(Event)) error {
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

	// A caller-supplied hook takes over the display; otherwise fall back
	// to the plain \r counter on stderr (verbose mode).
	stderrProgress := progress == nil && !quiet && resp.ContentLength > 0
	notify(progress, Event{Phase: PhaseDownload, Total: resp.ContentLength, URL: url, Path: dest})
	var src io.Reader = resp.Body
	if progress != nil || stderrProgress {
		src = &progressReader{
			r:        resp.Body,
			total:    resp.ContentLength,
			url:      url,
			dest:     dest,
			progress: progress,
			stderr:   stderrProgress,
		}
	}
	_, cerr := io.Copy(f, src)
	closeErr := f.Close()
	if stderrProgress {
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

// progressReader reports transfer progress while the body is copied.
// Updates are throttled so that a fast link does not spend its time
// formatting output.
type progressReader struct {
	r        io.Reader
	total    int64
	done     int64
	last     time.Time
	url      string
	dest     string
	progress func(Event)
	stderr   bool // use the plain \r counter instead of progress
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.done += int64(n)
	if time.Since(p.last) < 100*time.Millisecond && err != io.EOF {
		return n, err
	}
	p.last = time.Now()
	if p.stderr {
		if p.total > 0 {
			fmt.Fprintf(os.Stderr, "\r  %d / %d MiB (%d%%)",
				p.done>>20, p.total>>20, p.done*100/p.total)
		}
		return n, err
	}
	notify(p.progress, Event{
		Phase: PhaseDownload,
		Done:  p.done,
		Total: p.total,
		URL:   p.url,
		Path:  p.dest,
	})
	return n, err
}

// extractTarGz unpacks archive into destDir and returns the path to the
// top-level directory of the tarball (e.g. mongodb-linux-x86_64-ubuntu2204-8.3.4)
// together with a flag telling whether the archive was already unpacked
// and extraction was skipped.
//
// marker is a path relative to that top-level directory whose presence
// means the archive is already unpacked and extraction can be skipped
// (e.g. "bin/mongod" for a server tarball, "mongot" for Percona Search).
// guessName overrides the top-level directory name for that check; it is
// needed for archives whose top-level directory does not match the archive
// name. An empty guessName derives the name from the archive.
func extractTarGz(archive, destDir, marker, guessName string, quiet bool, progress func(Event)) (string, bool, error) {
	// Work with an absolute destination path. This is required for the
	// escape check below: with a relative destDir like ".",
	// filepath.Join(".", name) strips the leading "./", so the prefix
	// comparison would wrongly flag every entry as escaping.
	absDest, err := filepath.Abs(destDir)
	if err != nil {
		return "", false, err
	}
	destDir = absDest

	// Guess the top-level dir to allow skipping.
	name := guessName
	if name == "" {
		name = filepath.Base(archive)
		name = strings.TrimSuffix(name, ".tgz")
		name = strings.TrimSuffix(name, ".tar.gz")
	}
	guesses := []string{filepath.Join(destDir, name)}
	// Some archives unpack into a directory that does not match their
	// name (mongodb-macos-arm64-8.0.34.tgz holds mongodb-macos-aarch64--8.0.34/),
	// so also look where the archive's first entry would go.
	if guessName == "" {
		if top, err := archiveTopLevel(archive); err == nil && top != name {
			guesses = append(guesses, filepath.Join(destDir, top))
		}
	}
	for _, guess := range guesses {
		if _, err := os.Stat(filepath.Join(guess, marker)); err == nil {
			if !quiet {
				fmt.Fprintf(os.Stderr, "reusing extracted binaries in %s\n", guess)
			}
			notify(progress, Event{Phase: PhaseReuse, Path: guess})
			return guess, true, nil
		}
	}

	f, err := os.Open(archive)
	if err != nil {
		return "", false, err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", false, fmt.Errorf("%s: %w", archive, err)
	}
	defer gz.Close()

	if !quiet {
		fmt.Fprintf(os.Stderr, "extracting %s ...\n", archive)
	}
	notify(progress, Event{Phase: PhaseExtract, Path: archive})

	tr := tar.NewReader(gz)
	topLevel := ""
	var extracted int64

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", false, err
		}

		name := filepath.Clean(hdr.Name)
		if name == "." || strings.HasPrefix(name, "..") {
			continue
		}
		target := filepath.Join(destDir, name)
		if target != destDir && !strings.HasPrefix(target, destDir+string(os.PathSeparator)) {
			return "", false, fmt.Errorf("tar entry escapes destination: %s", hdr.Name)
		}
		if topLevel == "" {
			first := strings.SplitN(name, string(os.PathSeparator), 2)[0]
			topLevel = filepath.Join(destDir, first)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return "", false, err
			}
		case tar.TypeSymlink:
			os.Remove(target)
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return "", false, err
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return "", false, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return "", false, err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return "", false, err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return "", false, err
			}
			if err := out.Close(); err != nil {
				return "", false, err
			}
			extracted++
			if extracted%200 == 0 {
				notify(progress, Event{Phase: PhaseExtract, Done: extracted, Path: archive})
			}
		}
	}

	if topLevel == "" {
		return "", false, fmt.Errorf("archive %s appears to be empty", archive)
	}
	return topLevel, false, nil
}

// archiveTopLevel returns the top-level directory name of a .tar.gz
// archive, read from its first entry without unpacking anything.
func archiveTopLevel(archive string) (string, error) {
	f, err := os.Open(archive)
	if err != nil {
		return "", err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err != nil {
			return "", err
		}
		name := filepath.Clean(hdr.Name)
		if name == "." || strings.HasPrefix(name, "..") {
			continue
		}
		return strings.SplitN(name, string(os.PathSeparator), 2)[0], nil
	}
}
