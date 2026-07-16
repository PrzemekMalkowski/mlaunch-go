// Package platform detects the host OS distribution and CPU architecture
// and maps them to the tarball "variant" identifiers used by the MongoDB
// community download server (fastdl.mongodb.org) and by Percona downloads.
package platform

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Info describes the host platform.
type Info struct {
	OS         string // "linux" (only linux is supported for now)
	Arch       string // "x86_64" or "aarch64"
	DistroID   string // e.g. "ubuntu", "rhel", "centos", "debian", "amzn", "ol", "rocky", "almalinux"
	VersionID  string // e.g. "24.04", "9.3"
	Codename   string // e.g. "noble", "jammy" (Ubuntu/Debian)
	IDLike     string // contents of ID_LIKE, e.g. "rhel fedora"
	PrettyName string
}

// Detect inspects the running host.
func Detect() (*Info, error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("unsupported OS %q: mlaunch-go currently supports Linux only", runtime.GOOS)
	}

	info := &Info{OS: "linux"}

	switch runtime.GOARCH {
	case "amd64":
		info.Arch = "x86_64"
	case "arm64":
		info.Arch = "aarch64"
	default:
		return nil, fmt.Errorf("unsupported CPU architecture %q", runtime.GOARCH)
	}

	if err := info.readOSRelease("/etc/os-release"); err != nil {
		return nil, fmt.Errorf("cannot detect Linux distribution: %w", err)
	}
	return info, nil
}

func (i *Info) readOSRelease(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		val = strings.Trim(val, `"'`)
		switch key {
		case "ID":
			i.DistroID = strings.ToLower(val)
		case "VERSION_ID":
			i.VersionID = val
		case "VERSION_CODENAME", "UBUNTU_CODENAME":
			if i.Codename == "" {
				i.Codename = strings.ToLower(val)
			}
		case "ID_LIKE":
			i.IDLike = strings.ToLower(val)
		case "PRETTY_NAME":
			i.PrettyName = val
		}
	}
	return sc.Err()
}

// isRHELFamily reports whether the distro behaves like RHEL for the
// purposes of binary compatibility (RHEL, CentOS, Oracle Linux, Rocky,
// AlmaLinux, Amazon Linux is handled separately).
func (i *Info) isRHELFamily() bool {
	switch i.DistroID {
	case "rhel", "centos", "ol", "oracle", "rocky", "almalinux":
		return true
	}
	return strings.Contains(i.IDLike, "rhel") || strings.Contains(i.IDLike, "centos")
}

func (i *Info) isDebianLike() bool {
	if i.DistroID == "debian" {
		return true
	}
	return i.DistroID != "ubuntu" && strings.Contains(i.IDLike, "debian")
}

// majorVersion returns the numeric major version of VERSION_ID (9 for "9.3").
func (i *Info) majorVersion() int {
	major, _, _ := strings.Cut(i.VersionID, ".")
	n, err := strconv.Atoi(major)
	if err != nil {
		return 0
	}
	return n
}

// ubuntuNum converts "24.04" into 2404.
func (i *Info) ubuntuNum() int {
	parts := strings.SplitN(i.VersionID, ".", 2)
	if len(parts) != 2 {
		return 0
	}
	maj, err1 := strconv.Atoi(parts[0])
	min, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return 0
	}
	return maj*100 + min
}

// MongoDBVariantCandidates returns tarball variant strings for the
// community-edition download server, ordered from best match to broadest
// fallback. The caller is expected to probe them in order, because not
// every MongoDB release has a build for every distro release (e.g. some
// 8.x releases ship ubuntu2204 but not ubuntu2404 tarballs).
func (i *Info) MongoDBVariantCandidates() ([]string, error) {
	switch {
	case i.DistroID == "ubuntu":
		known := []int{2404, 2204, 2004, 1804, 1604}
		host := i.ubuntuNum()
		var out []string
		for _, v := range known {
			if host == 0 || v <= host {
				out = append(out, fmt.Sprintf("ubuntu%d", v))
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no MongoDB tarball variant known for Ubuntu %s", i.VersionID)
		}
		return out, nil

	case i.DistroID == "amzn":
		if i.majorVersion() >= 2023 {
			return []string{"amazon2023", "amazon2"}, nil
		}
		return []string{"amazon2"}, nil

	case i.isRHELFamily():
		switch m := i.majorVersion(); {
		case m >= 9:
			return []string{"rhel9", "rhel93", "rhel90", "rhel8", "rhel80"}, nil
		case m == 8:
			return []string{"rhel8", "rhel80", "rhel70"}, nil
		case m == 7:
			return []string{"rhel70"}, nil
		default:
			return nil, fmt.Errorf("no MongoDB tarball variant known for RHEL-like version %s", i.VersionID)
		}

	case i.isDebianLike():
		known := []int{12, 11, 10, 9}
		host := i.majorVersion()
		var out []string
		for _, v := range known {
			if host == 0 || v <= host {
				out = append(out, fmt.Sprintf("debian%d", v))
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no MongoDB tarball variant known for Debian %s", i.VersionID)
		}
		return out, nil

	case i.DistroID == "sles" || i.DistroID == "opensuse-leap" || strings.Contains(i.IDLike, "suse"):
		return []string{"suse15", "suse12"}, nil
	}

	return nil, fmt.Errorf("unsupported Linux distribution %q (ID_LIKE=%q); use --url or --binary-dir to override", i.DistroID, i.IDLike)
}

// PerconaFlavorCandidates returns the platform suffix candidates used in
// Percona Server for MongoDB "Linux - Generic" minimal tarball names,
// ordered from best match to broadest fallback:
//
//	percona-server-mongodb-8.0.4-1-x86_64.ol9-minimal.tar.gz
//	percona-server-mongodb-8.0.4-1-x86_64.jammy-minimal.tar.gz
func (i *Info) PerconaFlavorCandidates() ([]string, error) {
	switch {
	case i.DistroID == "ubuntu":
		// Map release number to codename, newest first, filtered to <= host.
		type rel struct {
			num  int
			name string
		}
		known := []rel{{2404, "noble"}, {2204, "jammy"}, {2004, "focal"}, {1804, "bionic"}}
		host := i.ubuntuNum()
		var out []string
		// Prefer the exact codename reported by the OS if present.
		if i.Codename != "" {
			out = append(out, i.Codename)
		}
		for _, r := range known {
			if (host == 0 || r.num <= host) && r.name != i.Codename {
				out = append(out, r.name)
			}
		}
		return out, nil

	case i.isRHELFamily() || i.DistroID == "amzn":
		switch m := i.majorVersion(); {
		case m >= 9 || m >= 2023: // amzn 2023 ~ el9
			return []string{"ol9", "ol8"}, nil
		case m == 8:
			return []string{"ol8", "ol7"}, nil
		case m == 7 || m == 2: // amzn 2 ~ el7
			return []string{"ol7"}, nil
		}
		return []string{"ol9", "ol8"}, nil

	case i.isDebianLike():
		type rel struct {
			num  int
			name string
		}
		known := []rel{{12, "bookworm"}, {11, "bullseye"}, {10, "buster"}}
		host := i.majorVersion()
		var out []string
		if i.Codename != "" {
			out = append(out, i.Codename)
		}
		for _, r := range known {
			if (host == 0 || r.num <= host) && r.name != i.Codename {
				out = append(out, r.name)
			}
		}
		return out, nil
	}

	return nil, fmt.Errorf("unsupported Linux distribution %q for Percona downloads; use --url or --binary-dir to override", i.DistroID)
}

func (i *Info) String() string {
	if i.PrettyName != "" {
		return fmt.Sprintf("%s (%s)", i.PrettyName, i.Arch)
	}
	return fmt.Sprintf("%s %s (%s)", i.DistroID, i.VersionID, i.Arch)
}
