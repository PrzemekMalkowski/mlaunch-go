package download

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/PrzemekMalkowski/mlaunch-go/internal/platform"
)

// DistSearch identifies Percona Search for MongoDB (mongot). It is not a
// server distribution: it cannot be passed to Resolve/Fetch, but it names
// the component in 'show-repo --distro search' and in the download cache.
const DistSearch Distribution = "search"

// DefaultSearchVersion is the Percona Search for MongoDB (mongot) release
// used when --search-version is not given. Percona Search is currently a
// Technical Preview and requires Percona Server for MongoDB 8.3+.
const DefaultSearchVersion = "1.70.3"

// mongotLauncher is the launcher script at the root of the extracted
// tarball; bin/jdk holds the bundled OpenJDK runtime, so no host JVM is
// required.
const mongotLauncher = "mongot"

// MongotOptions controls resolution and download of a Percona Search
// tarball.
type MongotOptions struct {
	// Version is the mongot release, e.g. "1.70.3". A package build
	// suffix ("-1") is accepted and stripped: it appears in the RPM/DEB
	// version but not in the tarball name.
	Version string
	// DownloadDir is where the tarball is stored and extracted.
	// Defaults to ".".
	DownloadDir string
	// URL, when non-empty, skips resolution entirely.
	URL string
	// Quiet suppresses progress output.
	Quiet bool
}

// ResolveMongot computes the tarball URL for the given options on the given
// host. Unlike the server tarballs, mongot builds are distro-agnostic (the
// JDK is bundled), so only the architecture varies and there is nothing to
// probe:
//
//	https://downloads.percona.com/downloads/percona-search-mongodb/
//	  percona-search-mongodb-1.70.3/binary/tarball/
//	  percona-search-mongodb-1.70.3-linux_x86_64.tar.gz
func ResolveMongot(opts MongotOptions, host *platform.Info) (string, error) {
	if opts.URL != "" {
		return opts.URL, nil
	}
	v, err := mongotTarballVersion(opts.Version)
	if err != nil {
		return "", err
	}
	switch host.Arch {
	case "x86_64", "aarch64":
	default:
		return "", fmt.Errorf("no Percona Search build for architecture %q", host.Arch)
	}
	return fmt.Sprintf(
		"https://downloads.percona.com/downloads/percona-search-mongodb/"+
			"percona-search-mongodb-%s/binary/tarball/"+
			"percona-search-mongodb-%s-linux_%s.tar.gz",
		v, v, host.Arch), nil
}

// mongotTarballVersion normalises "1.70.3-1" to "1.70.3".
func mongotTarballVersion(version string) (string, error) {
	v := version
	if v == "" {
		v = DefaultSearchVersion
	}
	base, _, _ := strings.Cut(v, "-")
	if strings.Count(base, ".") != 2 {
		return "", fmt.Errorf("invalid Percona Search version %q, expected e.g. 1.70.3", version)
	}
	return base, nil
}

// FetchMongot downloads and extracts the mongot tarball, reusing an
// existing archive or extraction when present. The returned BinDir is the
// directory holding the mongot launcher script.
//
// The tarball's top-level directory is version-less ("percona-search-mongodb"),
// so each release is extracted into its own versioned parent directory to
// keep several versions side by side in one download dir.
func FetchMongot(opts MongotOptions, host *platform.Info) (*Result, error) {
	url, err := ResolveMongot(opts, host)
	if err != nil {
		return nil, err
	}
	v, err := mongotTarballVersion(opts.Version)
	if err != nil {
		return nil, err
	}

	dir := opts.DownloadDir
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	local := filepath.Join(dir, filepath.Base(url))
	if st, err := os.Stat(local); err == nil && st.Size() > 0 {
		if !opts.Quiet {
			fmt.Fprintf(os.Stderr, "reusing existing archive %s\n", local)
		}
	} else if err := downloadFile(url, local, opts.Quiet); err != nil {
		return nil, err
	}

	destDir := filepath.Join(dir, "percona-search-mongodb-"+v)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return nil, err
	}
	binDir, err := extractTarGz(local, destDir, mongotLauncher, "percona-search-mongodb", opts.Quiet)
	if err != nil {
		return nil, err
	}
	if err := ValidateMongotDir(binDir); err != nil {
		return nil, err
	}
	return &Result{URL: url, Archive: local, BinDir: binDir}, nil
}

// ValidateMongotDir checks that dir looks like an extracted Percona Search
// tarball: the launcher script plus the bundled OpenJDK runtime.
func ValidateMongotDir(dir string) error {
	for _, rel := range []string{
		mongotLauncher,
		filepath.Join("bin", "jdk", "bin", "java"),
	} {
		if _, err := os.Stat(filepath.Join(dir, rel)); err != nil {
			return fmt.Errorf("%s does not look like a Percona Search directory (missing %s): %w", dir, rel, err)
		}
	}
	return nil
}
