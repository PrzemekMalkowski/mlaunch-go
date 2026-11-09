// Package cluster launches and manages sandbox MongoDB deployments,
// mirroring the behaviour of mlaunch's init/start/stop/list commands.
//
// Sandboxes live under a common root directory. Each environment gets its
// own subdirectory named after its type, total member count, and version,
// e.g. "replicaset-3n-8.0.26". Default port numbers are derived from the
// version so that sandboxes of different versions do not collide.
//
// Every instance is configured through its own generated mongod.conf /
// mongos.conf (YAML). The files can be edited by hand; changes apply on
// the next 'mlaunch-go start'.
package cluster

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/PrzemekMalkowski/mlaunch-go/internal/mongo"
	"github.com/PrzemekMalkowski/mlaunch-go/internal/table"
)

// defaultCacheSizeGB caps the WiredTiger cache of every mongod. Left to
// itself each mongod sizes its cache as if it had the host to itself
// (50% of RAM minus 1 GB), which overcommits memory as soon as a sandbox
// runs several of them.
const defaultCacheSizeGB = 0.5

// StateFileName mirrors mlaunch's .mlaunch_startup marker.
const StateFileName = ".mlaunch_startup.json"

// Default credentials created with --auth. The password intentionally
// avoids shell/URI special characters, so it is safe to paste anywhere.
const (
	DefaultUser     = "mlaunch"
	DefaultPassword = "pa33word"
)

// Topology describes what the user asked for.
type Topology struct {
	Single     bool
	ReplicaSet bool
	Nodes      int    // data-bearing nodes per replica set
	Arbiters   int    // arbiters per replica set (replicaset and shard replsets)
	Name       string // replica set name
	Sharded    int    // number of shards; 0 = unsharded
	ConfigSrv  int    // config servers (CSRS members)
	Mongos     int    // number of mongos routers
	Auth       bool   // enable authentication, create the default user
	Port       int    // base port; 0 = derive from version
	RootDir    string // sandboxes root; the env dir is created inside
	BinDir     string // directory containing mongod / mongos
	Version    string // server version, used for naming and port derivation
	ExtraArgs  []string
	Verbose    bool

	// Search launches Percona Search (mongot) alongside the servers. It
	// implies Auth and a replica set, and requires Percona Server for
	// MongoDB 8.3 or later. See mongot.go.
	Search        bool
	SearchBinDir  string // directory containing the mongot launcher
	SearchVersion string // mongot version, for the summary only
	// SearchDiskThreshold is the sandbox volume usage fraction above
	// which init warns; 0 means DefaultSearchDiskThreshold.
	SearchDiskThreshold float64

	// dir is the resolved per-environment directory (inside RootDir).
	dir string
	// keyFile is the generated keyfile path when Auth is set for a
	// multi-process topology.
	keyFile string
}

// Node is one launched process.
type Node struct {
	Name     string   `json:"name"`
	Kind     string   `json:"kind"` // "mongod" or "mongos"
	Port     int      `json:"port"`
	DBPath   string   `json:"dbpath,omitempty"`
	LogPath  string   `json:"logpath"`
	ConfPath string   `json:"confPath"`
	Args     []string `json:"args"`
	PID      int      `json:"pid"`

	// mongot only: Port holds the gRPC port mongod talks to, these are
	// the health-check and metrics listeners.
	HealthPort  int `json:"healthPort,omitempty"`
	MetricsPort int `json:"metricsPort,omitempty"`
}

// State is persisted to <envdir>/.mlaunch_startup.json.
type State struct {
	CreatedAt time.Time `json:"createdAt"`
	BinDir    string    `json:"binDir"`
	Topology  string    `json:"topology"` // "standalone" | "replicaset" | "sharded"
	Version   string    `json:"version"`
	Members   int       `json:"members"`
	ReplSet   string    `json:"replSet,omitempty"`
	BasePort  int       `json:"basePort"`
	Auth      bool      `json:"auth,omitempty"`
	KeyFile   string    `json:"keyFile,omitempty"`
	// Search records that mongot processes belong to this environment;
	// SearchBinDir is needed to restart them.
	Search       bool   `json:"search,omitempty"`
	SearchBinDir string `json:"searchBinDir,omitempty"`
	// Complete is false while init is still launching processes; a state
	// file with Complete=false marks a failed/interrupted init.
	Complete bool    `json:"complete"`
	Nodes    []*Node `json:"nodes"`

	// Dir is not persisted; it is filled in when the state is loaded.
	Dir string `json:"-"`
}

// EnvName builds the canonical sandbox directory name,
// e.g. "standalone-1n-8.0.26", "replicaset-3n-8.0.26", "sharded-8n-8.0.26".
func EnvName(topology string, members int, version string) string {
	v := version
	if v == "" {
		v = "custom"
	}
	return fmt.Sprintf("%s-%dn-%s", topology, members, v)
}

func statePath(dir string) string { return filepath.Join(dir, StateFileName) }

// LoadState reads the state file of a single environment directory.
func LoadState(dir string) (*State, error) {
	data, err := os.ReadFile(statePath(dir))
	if err != nil {
		return nil, err
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("corrupt state file %s: %w", statePath(dir), err)
	}
	st.Dir = dir
	return &st, nil
}

func (s *State) save(dir string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(statePath(dir), data, 0o644)
}

// EnvDirs returns all environment directories under root that contain a
// state file. If root itself contains a state file, only root is returned
// (allows pointing --dir at an env directly).
func EnvDirs(root string) ([]string, error) {
	if _, err := os.Stat(statePath(root)); err == nil {
		return []string{root}, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var dirs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		d := filepath.Join(root, e.Name())
		if _, err := os.Stat(statePath(d)); err == nil {
			dirs = append(dirs, d)
		}
	}
	sort.Strings(dirs)
	return dirs, nil
}

// OrphanDirs returns subdirectories of root that have no state file —
// typically leftovers of failed inits from older versions of the tool.
func OrphanDirs(root string) ([]string, error) {
	if _, err := os.Stat(statePath(root)); err == nil {
		return nil, nil // root is itself an env
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var dirs []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		d := filepath.Join(root, e.Name())
		if _, err := os.Stat(statePath(d)); err == nil {
			continue
		}
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	return dirs, nil
}

// ResolveEnv finds the environment directory to operate on. env may be a
// sandbox name (subdirectory of root) or empty; when empty and exactly one
// environment exists under root, that one is chosen. A named env is also
// accepted when it has no state file (orphan), so it can be destroyed.
func ResolveEnv(root, env string) (string, error) {
	dirs, err := EnvDirs(root)
	if err != nil {
		return "", err
	}
	if env != "" {
		want := filepath.Join(root, env)
		for _, d := range dirs {
			if d == want || filepath.Base(d) == env {
				return d, nil
			}
		}
		// Orphan: existing directory without a state file.
		if fi, err := os.Stat(want); err == nil && fi.IsDir() {
			return want, nil
		}
		return "", fmt.Errorf("no environment named %q under %s", env, root)
	}
	switch len(dirs) {
	case 0:
		return "", fmt.Errorf("no mlaunch-go environments found under %s", root)
	case 1:
		return dirs[0], nil
	default:
		names := make([]string, len(dirs))
		for i, d := range dirs {
			names[i] = filepath.Base(d)
		}
		return "", fmt.Errorf("multiple environments under %s; pick one with --env: %s",
			root, strings.Join(names, ", "))
	}
}

// ---------------------------------------------------------------- port plan

var versionRe = regexp.MustCompile(`(\d+)\.(\d+)\.(\d+)`)

// VersionBasePort derives a deterministic default base port from a version
// string: 20000 + (major*1000 + minor*100 + patch) % 40000.
// Examples: 8.0.26 -> 28026, 7.0.14 -> 27014, 6.0.21 -> 26021.
// Falls back to 27017 when the version cannot be parsed.
func VersionBasePort(version string) int {
	m := versionRe.FindStringSubmatch(version)
	if m == nil {
		return 27017
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	patch, _ := strconv.Atoi(m[3])
	return 20000 + (major*1000+minor*100+patch)%40000
}

// planPorts returns a base port such that [base, base+count) is free: every
// port must be bindable right now and must not be recorded by any other
// sandbox under root (running or stopped, so a stopped sandbox can still be
// started later without clashes). Starting from preferred, the span is
// shifted by +100 (wrapping within 20000..60000) until one fits.
func planPorts(root string, preferred, count int) (int, error) {
	reserved := map[int]bool{}
	dirs, _ := EnvDirs(root)
	for _, d := range dirs {
		if st, err := LoadState(d); err == nil {
			for _, n := range st.Nodes {
				reserved[n.Port] = true
				if n.HealthPort > 0 {
					reserved[n.HealthPort] = true
				}
				if n.MetricsPort > 0 {
					reserved[n.MetricsPort] = true
				}
			}
		}
	}

	base := preferred
	for attempt := 0; attempt < 400; attempt++ {
		if base < 1024 || base+count > 65535 {
			base = 20000
		}
		ok := true
		for p := base; p < base+count; p++ {
			if reserved[p] || !portFree(p) {
				ok = false
				break
			}
		}
		if ok {
			return base, nil
		}
		base += 100
		if base >= 60000 {
			base = 20000 + (base % 100)
		}
	}
	return 0, fmt.Errorf("could not find %d consecutive free ports (tried starting at %d)", count, preferred)
}

func portFree(port int) bool {
	l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		return false
	}
	l.Close()
	return true
}

// ---------------------------------------------------------------- init

// TotalMembers computes the number of processes the topology will launch.
func (t *Topology) TotalMembers() int {
	switch {
	case t.Sharded > 0:
		return t.Sharded*(t.Nodes+t.Arbiters) + t.ConfigSrv + t.Mongos
	case t.ReplicaSet:
		return t.Nodes + t.Arbiters
	default:
		return 1
	}
}

func (t *Topology) kind() string {
	switch {
	case t.Sharded > 0:
		return "sharded"
	case t.ReplicaSet:
		return "replicaset"
	default:
		return "standalone"
	}
}

// Init creates the environment directory, plans ports, launches the
// requested topology and initialises replica sets / sharding / auth.
func Init(t *Topology) (*State, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}
	if t.Search {
		warnSearchResources(t)
	}

	envName := EnvName(t.kind(), t.TotalMembers(), t.Version)
	if t.Search {
		// mongot processes are not counted as members, so the
		// environment is distinguished by a suffix instead.
		envName += "-search"
	}
	t.dir = filepath.Join(t.RootDir, envName)
	if _, err := LoadState(t.dir); err == nil {
		return nil, fmt.Errorf("environment %s already exists in %s; use 'mlaunch-go start --env %s' or 'mlaunch-go destroy --env %s'",
			envName, t.RootDir, envName, envName)
	}
	if err := os.MkdirAll(t.dir, 0o755); err != nil {
		return nil, err
	}

	// Plan the port span.
	preferred := t.Port
	if preferred <= 0 {
		preferred = VersionBasePort(t.Version)
	}
	base, err := planPorts(t.RootDir, preferred, t.TotalPorts())
	if err != nil {
		return nil, err
	}
	t.Port = base
	if t.Verbose {
		fmt.Fprintf(os.Stderr, "environment %s: ports %d-%d\n", envName, base, base+t.TotalPorts()-1)
	}

	// Keyfile for internal auth (multi-process topologies only).
	if t.Auth && t.kind() != "standalone" {
		t.keyFile = filepath.Join(t.dir, "keyfile")
		if err := writeKeyFile(t.keyFile); err != nil {
			return nil, err
		}
	}

	// Password file read by mongot (no trailing newline, or it would
	// become part of the password).
	if t.Search {
		if err := writeSearchPasswordFile(filepath.Join(t.dir, searchPasswordFile)); err != nil {
			return nil, err
		}
	}

	st := &State{
		CreatedAt: time.Now().UTC(),
		BinDir:    t.BinDir,
		Topology:  t.kind(),
		Version:   t.Version,
		Members:   t.TotalMembers(),
		BasePort:  base,
		Auth:      t.Auth,
		KeyFile:   t.keyFile,
		Dir:       t.dir,

		Search:       t.Search,
		SearchBinDir: t.SearchBinDir,
	}
	// Persist immediately and after every spawn (see spawn()), so that a
	// failed init leaves a state file behind: 'list' shows it as
	// incomplete and 'destroy' can clean it up.
	if err := st.save(t.dir); err != nil {
		return nil, err
	}
	table.StepDone("Prepared %s (%d process(es), ports %d-%d)",
		table.Bold(envName), t.TotalMembers(), base, base+t.TotalPorts()-1)

	var connAddr string
	switch {
	case t.Sharded > 0:
		connAddr, err = initSharded(t, st)
	case t.ReplicaSet:
		st.ReplSet = t.Name
		connAddr, err = initReplicaSet(t, st)
	default:
		connAddr, err = initSingle(t, st)
	}
	if err != nil {
		// Best effort teardown of whatever managed to start; the state
		// file (Complete=false) is kept so 'destroy' can remove the dir.
		for _, n := range st.Nodes {
			if n.PID > 0 {
				syscall.Kill(n.PID, syscall.SIGKILL)
			}
		}
		st.save(t.dir)
		return nil, fmt.Errorf("%w\nthe partial environment was kept; remove it with: mlaunch-go destroy --env %s", err, envName)
	}

	st.Complete = true
	if err := st.save(t.dir); err != nil {
		return nil, err
	}

	// Visually separate the init progress log from the final summary.
	fmt.Println(table.Rule(72))
	if connAddr != "" {
		table.Ready("Sandbox ready! Connect via: %s", table.Cyan(connString(st, connAddr)))
	} else {
		table.Ready("Sandbox %s ready!", envName)
	}
	summaryLine("environment", table.Bold(envName))
	summaryLine("directory", t.dir)
	summaryLine("processes", fmt.Sprintf("%d, ports from %d", len(st.Nodes), base))
	summaryLine("state file", statePath(t.dir))
	if t.Auth {
		summaryLine("auth", fmt.Sprintf("enabled; default user '%s' / '%s'", DefaultUser, DefaultPassword))
	}
	if t.Search {
		summaryLine("search", fmt.Sprintf("%d mongot process(es), replication user '%s'",
			t.searchInstances(), SearchUser))
	}
	return st, nil
}

// summaryLine prints one "  label:  value" row of the final summary, with
// the labels dimmed and aligned.
func summaryLine(label, value string) {
	fmt.Printf("  %s %s\n", table.Dim(fmt.Sprintf("%-12s", label+":")), value)
}

func (t *Topology) validate() error {
	if t.BinDir == "" {
		return fmt.Errorf("no binary directory resolved")
	}
	if _, err := os.Stat(filepath.Join(t.BinDir, "mongod")); err != nil {
		return fmt.Errorf("mongod not found in %s: %w", t.BinDir, err)
	}
	if t.Sharded > 0 {
		if _, err := os.Stat(filepath.Join(t.BinDir, "mongos")); err != nil {
			return fmt.Errorf("mongos not found in %s (required for --sharded): %w", t.BinDir, err)
		}
	}
	if t.Single && t.ReplicaSet {
		return fmt.Errorf("--single and --replicaset are mutually exclusive")
	}
	if t.Search {
		if t.Single {
			return fmt.Errorf("--percona-search cannot be combined with --single: mongot replicates through a change stream, so a replica set (or sharded cluster) is required")
		}
		if t.Sharded == 0 {
			t.ReplicaSet = true // implied
		}
		t.Auth = true // mongot authenticates with SCRAM; implied
		if t.SearchBinDir == "" {
			return fmt.Errorf("no Percona Search binary directory resolved")
		}
		if _, err := os.Stat(filepath.Join(t.SearchBinDir, "mongot")); err != nil {
			return fmt.Errorf("mongot not found in %s: %w", t.SearchBinDir, err)
		}
		if !searchCompatible(t.Version) {
			return fmt.Errorf("Percona Search requires Percona Server for MongoDB 8.3 or later (got version %q); see: mlaunch-go show-repo --distro percona --major 8.3", t.Version)
		}
	}
	if t.Arbiters < 0 {
		return fmt.Errorf("--arbiter must be >= 0")
	}
	if t.Arbiters > 0 && !t.ReplicaSet && t.Sharded == 0 {
		return fmt.Errorf("--arbiter requires --replicaset or --sharded")
	}
	if t.RootDir == "" {
		t.RootDir = "./sandboxes"
	}
	if t.Nodes < 1 {
		t.Nodes = 3
	}
	if t.Name == "" {
		t.Name = "replset"
	}
	if t.ConfigSrv < 1 {
		t.ConfigSrv = 1
	}
	if t.Mongos < 1 {
		t.Mongos = 1
	}
	return nil
}

func writeKeyFile(path string) error {
	raw := make([]byte, 96)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	content := base64.StdEncoding.EncodeToString(raw) + "\n"
	return os.WriteFile(path, []byte(content), 0o600)
}

// ------------------------------------------------------------- config files

// mongodConf renders the per-instance mongod.conf. Optional sections that
// are not in use are included as commented-out template hints, so the file
// doubles as a starting point for manual tweaking.
func mongodConf(t *Topology, name, dbpath, logPath string, port int, replSet, clusterRole, mongotAddr string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# mongod.conf for instance %q, generated by mlaunch-go.\n", name)
	b.WriteString("# Edit freely; changes apply on the next 'mlaunch-go start'.\n")
	b.WriteString("# Reference: https://www.mongodb.com/docs/manual/reference/configuration-options/\n\n")

	fmt.Fprintf(&b, "storage:\n  dbPath: %s\n", dbpath)
	fmt.Fprintf(&b, "  wiredTiger:\n    engineConfig:\n      cacheSizeGB: %g\n\n", defaultCacheSizeGB)

	fmt.Fprintf(&b, "systemLog:\n  destination: file\n  path: %s\n  logAppend: true\n", logPath)
	b.WriteString("  # verbosity: 1\n\n")

	b.WriteString("processManagement:\n  fork: true\n\n")

	fmt.Fprintf(&b, "net:\n  port: %d\n  bindIp: 127.0.0.1\n\n", port)

	if replSet != "" {
		fmt.Fprintf(&b, "replication:\n  replSetName: %s\n", replSet)
		b.WriteString("  # oplogSizeMB: 128\n\n")
	} else {
		b.WriteString("# replication:\n#   replSetName: replset\n#   oplogSizeMB: 128\n\n")
	}

	if clusterRole != "" {
		fmt.Fprintf(&b, "sharding:\n  clusterRole: %s\n\n", clusterRole)
	}

	if t.Auth {
		b.WriteString("security:\n  authorization: enabled\n")
		if t.keyFile != "" {
			fmt.Fprintf(&b, "  keyFile: %s\n", t.keyFile)
		}
		b.WriteString("\n")
	} else {
		b.WriteString("# security:\n#   authorization: enabled\n#   keyFile: /path/to/keyfile\n\n")
	}

	// mongot wiring. These are startup parameters and mongod tolerates
	// the endpoint not listening yet, which is why the mongot ports are
	// planned before anything is launched. mongotHost must match
	// searchIndexManagementHostAndPort.
	if mongotAddr != "" {
		b.WriteString("setParameter:\n")
		fmt.Fprintf(&b, "  searchIndexManagementHostAndPort: %s\n", mongotAddr)
		fmt.Fprintf(&b, "  mongotHost: %s\n", mongotAddr)
		b.WriteString("  skipAuthenticationToSearchIndexManagementServer: false\n")
		b.WriteString("  useGrpcForSearch: true\n")
		b.WriteString("  searchTLSMode: disabled\n")
	} else {
		b.WriteString("# setParameter:\n#   enableFlowControl: true\n")
	}
	return b.String()
}

// mongosConf renders the per-router mongos.conf. mongotAddr enables the
// search index management commands on the router: without it mongos rejects
// createSearchIndexes / $listSearchIndexes as unconfigured, even though the
// shards behind it are wired to their own mongot.
func mongosConf(t *Topology, name, logPath string, port int, configDB, mongotAddr string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# mongos.conf for instance %q, generated by mlaunch-go.\n", name)
	b.WriteString("# Edit freely; changes apply on the next 'mlaunch-go start'.\n\n")

	fmt.Fprintf(&b, "systemLog:\n  destination: file\n  path: %s\n  logAppend: true\n\n", logPath)
	b.WriteString("processManagement:\n  fork: true\n\n")
	fmt.Fprintf(&b, "net:\n  port: %d\n  bindIp: 127.0.0.1\n\n", port)
	fmt.Fprintf(&b, "sharding:\n  configDB: %s\n\n", configDB)

	if mongotAddr != "" {
		// Index management is metadata work that mongos fans out to the
		// shards, each of which talks to its own mongot; the address here
		// only has to name a reachable mongot (the first shard's).
		b.WriteString("setParameter:\n")
		fmt.Fprintf(&b, "  searchIndexManagementHostAndPort: %s\n", mongotAddr)
		fmt.Fprintf(&b, "  mongotHost: %s\n", mongotAddr)
		b.WriteString("  skipAuthenticationToSearchIndexManagementServer: false\n")
		b.WriteString("  useGrpcForSearch: true\n")
		b.WriteString("  searchTLSMode: disabled\n\n")
	}

	if t.Auth && t.keyFile != "" {
		fmt.Fprintf(&b, "security:\n  keyFile: %s\n", t.keyFile)
	} else {
		b.WriteString("# security:\n#   keyFile: /path/to/keyfile\n")
	}
	return b.String()
}

// ---------------------------------------------------------------- topologies

func initSingle(t *Topology, st *State) (string, error) {
	sp := table.StartStep("Starting mongod on port %d...", t.Port)
	n, err := launchMongod(t, st, "single", t.Port, filepath.Join(t.dir, "data", "db"), "", "", "")
	if err != nil {
		sp.Fail("mongod failed to start on port %d", t.Port)
		return "", err
	}
	table.Detail("waiting for connections")
	if err := mongo.WaitReachable(hostPort(n.Port), 60*time.Second); err != nil {
		sp.Fail("mongod on port %d did not accept connections", n.Port)
		return "", err
	}
	sp.Done("Standalone mongod ready on port %d (pid %d)", n.Port, n.PID)

	if t.Auth {
		if err := createUserStep(hostPort(n.Port)); err != nil {
			return "", err
		}
	}
	return hostPort(n.Port), nil
}

// createUserStep creates the default user, reported as its own stage.
func createUserStep(addr string) error {
	sp := table.StartStep("Creating user %q...", DefaultUser)
	if err := createDefaultUser(addr); err != nil {
		sp.Fail("Could not create user %q", DefaultUser)
		return err
	}
	sp.Done("Authentication enabled, user %q created", DefaultUser)
	return nil
}

func initReplicaSet(t *Topology, st *State) (string, error) {
	if t.Search {
		table.StepDone("Configured mongod to connect to mongot on %s", t.mongotAddr(0))
	}

	sp := table.StartStep("Starting replica set %q (%s)...", t.Name, memberSummary(t.Nodes, t.Arbiters))
	dataPorts, arbPorts, err := launchReplSetMembers(t, st, t.Name, t.Port, t.Nodes, t.Arbiters, "", t.mongotAddr(0))
	if err != nil {
		sp.Fail("Could not start the members of replica set %q", t.Name)
		return "", err
	}
	sp.Done("Started %s on port(s) %s", memberSummary(t.Nodes, t.Arbiters),
		joinPorts(append(append([]int{}, dataPorts...), arbPorts...)))

	sp = table.StartStep("Initiating replica set %q...", t.Name)
	table.Detail("waiting for a primary")
	primary, err := initiateReplSet(t.Name, dataPorts, arbPorts, false)
	if err != nil {
		sp.Fail("Replica set %q did not come up", t.Name)
		return "", err
	}
	sp.Done("Replica set %q initiated, primary on %s", t.Name, primary)

	if t.Auth {
		if err := createUserStep(primary); err != nil {
			return "", err
		}
	}
	if t.Search {
		sp = table.StartStep("Starting mongot process...")
		if err := createSearchUser(primary); err != nil {
			sp.Fail("Could not create the mongot replication user")
			return "", err
		}
		n, err := startSearch(t, st, 0, "mongot", primary, "")
		if err != nil {
			sp.Fail("mongot failed to start")
			return "", err
		}
		sp.Done("mongot ready on port %d (health %d, metrics %d, pid %d)",
			n.Port, n.HealthPort, n.MetricsPort, n.PID)
	}
	return primary, nil
}

// memberSummary describes a replica set's composition, e.g.
// "3 data nodes" or "2 data nodes + 1 arbiter".
func memberSummary(nodes, arbiters int) string {
	s := fmt.Sprintf("%d data node", nodes)
	if nodes != 1 {
		s += "s"
	}
	if arbiters > 0 {
		s += fmt.Sprintf(" + %d arbiter", arbiters)
		if arbiters != 1 {
			s += "s"
		}
	}
	return s
}

func initSharded(t *Topology, st *State) (string, error) {
	port := t.Port

	// 1. Shards: each shard is a replica set named sh01, sh02, ...
	// With --auth, the default user is also created on each shard's
	// primary (via the localhost exception, before internal auth locks
	// direct access down): cluster-level users created through mongos
	// live on the config servers only, so shard-local users are needed
	// for direct maintenance connections to individual shards.
	var shardSpecs []string
	// With --percona-search, the shard primaries mongot will replicate
	// from, collected here and used after the routers are up.
	var shardPrimaries, shardNames []string
	if t.Search {
		table.StepDone("Configured mongod to connect to mongot on %s and up",
			t.mongotAddr(0))
	}
	for s := 1; s <= t.Sharded; s++ {
		rsName := fmt.Sprintf("sh%02d", s)
		sp := table.StartStep("Starting shard %s of %d (%s)...", rsName, t.Sharded,
			memberSummary(t.Nodes, t.Arbiters))
		dataPorts, arbPorts, err := launchReplSetMembers(t, st, rsName, port, t.Nodes, t.Arbiters, "shardsvr", t.mongotAddr(s-1))
		if err != nil {
			sp.Fail("Could not start the members of shard %s", rsName)
			return "", err
		}
		port += t.Nodes + t.Arbiters
		table.Detail("initiating replica set, waiting for a primary")
		primary, err := initiateReplSet(rsName, dataPorts, arbPorts, false)
		if err != nil {
			sp.Fail("Shard %s did not come up", rsName)
			return "", err
		}
		if t.Auth {
			table.Detail("creating shard-local user %q", DefaultUser)
			if err := createDefaultUser(primary); err != nil {
				sp.Fail("Could not create the shard-local user on %s", rsName)
				return "", fmt.Errorf("shard-local user on %s: %w", rsName, err)
			}
		}
		// Each shard gets its own mongot, replicating from that shard's
		// primary; the config servers need none. The processes cannot be
		// launched yet: in a sharded cluster mongot also needs a router,
		// and no mongos exists until step 3. Only the shard-local part of
		// the setup (the replication user) is done here, while the
		// localhost exception makes it cheap.
		if t.Search {
			table.Detail("creating the mongot replication user")
			if err := createSearchUser(primary); err != nil {
				sp.Fail("Could not create the mongot user on %s", rsName)
				return "", fmt.Errorf("search user on %s: %w", rsName, err)
			}
			shardPrimaries = append(shardPrimaries, primary)
			shardNames = append(shardNames, rsName)
		}
		// Only data-bearing members go into the addShard seed list.
		shardSpecs = append(shardSpecs, rsName+"/"+joinHostPorts(dataPorts))
		sp.Done("Shard %s ready, primary on %s", rsName, primary)
	}
	if t.Auth {
		table.StepDone("Shard-local user %q created on every shard (for direct connections)", DefaultUser)
	}

	// 2. Config server replica set (arbiters are not supported on CSRS).
	csName := "configRepl"
	sp := table.StartStep("Starting %d config server(s)...", t.ConfigSrv)
	csPorts, _, err := launchReplSetMembers(t, st, csName, port, t.ConfigSrv, 0, "configsvr", "")
	if err != nil {
		sp.Fail("Could not start the config servers")
		return "", err
	}
	port += t.ConfigSrv
	table.Detail("initiating %s, waiting for a primary", csName)
	if _, err := initiateReplSet(csName, csPorts, nil, true); err != nil {
		sp.Fail("Config server replica set %s did not come up", csName)
		return "", err
	}
	sp.Done("Config server replica set %s ready on port(s) %s", csName, joinPorts(csPorts))

	// 3. mongos routers.
	sp = table.StartStep("Starting %d mongos router(s)...", t.Mongos)
	var mongosPorts []int
	for m := 1; m <= t.Mongos; m++ {
		// The mongot address is a startup parameter only: the processes
		// themselves are launched in step 7, after the routers exist.
		var mongotAddr string
		if t.Search {
			mongotAddr = t.mongotAddr(0)
		}
		table.Detail("mongos%d on port %d", m, port)
		n, err := launchMongos(t, st, fmt.Sprintf("mongos%d", m), port,
			csName+"/"+joinHostPorts(csPorts), mongotAddr)
		if err != nil {
			sp.Fail("mongos%d failed to start on port %d", m, port)
			return "", err
		}
		mongosPorts = append(mongosPorts, n.Port)
		port++
	}
	table.Detail("waiting for connections")
	for _, p := range mongosPorts {
		if err := mongo.WaitReachable(hostPort(p), 90*time.Second); err != nil {
			sp.Fail("mongos on port %d did not accept connections", p)
			return "", err
		}
	}
	sp.Done("Started %d mongos router(s) on port(s) %s", t.Mongos, joinPorts(mongosPorts))

	// 4. With auth: create the cluster-wide default user via the first
	// mongos while the localhost exception is still open, then
	// authenticate for addShard.
	c, err := mongo.Dial(hostPort(mongosPorts[0]), 10*time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	if t.Auth {
		sp = table.StartStep("Creating user %q...", DefaultUser)
		if err := createDefaultUser(hostPort(mongosPorts[0])); err != nil {
			sp.Fail("Could not create user %q", DefaultUser)
			return "", err
		}
		if err := c.Authenticate("admin", DefaultUser, DefaultPassword); err != nil {
			sp.Fail("Could not authenticate as %q", DefaultUser)
			return "", err
		}
		sp.Done("Authentication enabled, user %q created", DefaultUser)
	}

	// 5. Shards with arbiters can degrade the implicit default write
	// concern to {w: 1}; since MongoDB 5.0 addShard rejects such shards
	// unless a cluster-wide default write concern is set explicitly.
	if t.Arbiters > 0 {
		_, err := c.RunCommand("admin", mongo.D{
			{"setDefaultRWConcern", 1},
			{"defaultWriteConcern", mongo.D{{"w", 1}}},
		})
		if err != nil {
			return "", fmt.Errorf("setDefaultRWConcern (required for shards with arbiters): %w", err)
		}
		table.StepDone("Cluster-wide default write concern set to {w: 1} (shards contain arbiters)")
	}

	// 6. Register shards through the first mongos.
	sp = table.StartStep("Adding %d shard(s) to the cluster...", len(shardSpecs))
	for i, spec := range shardSpecs {
		table.Detail("shard %d/%d", i+1, len(shardSpecs))
		if _, err := c.RunCommand("admin", mongo.D{{"addShard", spec}}); err != nil {
			sp.Fail("Could not add shard %s", spec)
			return "", fmt.Errorf("addShard %s: %w", spec, err)
		}
	}
	sp.Done("Added %d shard(s) to the cluster", len(shardSpecs))
	// 7. One mongot per shard, now that the cluster is complete: mongot
	// checks at bootstrap that its config matches the real topology, and
	// a shard only reports itself as sharded once it has been added.
	// The replication user was created shard-locally in step 1; mongot
	// also authenticates against the router, and cluster users live on
	// the config servers, so it has to be created through mongos as well.
	if t.Search {
		router := hostPort(mongosPorts[0])
		sp = table.StartStep("Starting %d mongot process(es)...", len(shardNames))
		if err := createSearchUser(router); err != nil {
			sp.Fail("Could not create the mongot user through mongos")
			return "", fmt.Errorf("search user via mongos: %w", err)
		}
		for i, rsName := range shardNames {
			table.Detail("mongot for shard %s (%d/%d)", rsName, i+1, len(shardNames))
			n, err := startSearch(t, st, i, "mongot-"+rsName, shardPrimaries[i], router)
			if err != nil {
				sp.Fail("mongot for shard %s failed to start", rsName)
				return "", fmt.Errorf("mongot for %s: %w", rsName, err)
			}
			sp.Note("mongot for shard %s ready on port %d (health %d, metrics %d, pid %d)",
				rsName, n.Port, n.HealthPort, n.MetricsPort, n.PID)
		}
		sp.Done("Started %d mongot process(es), one per shard", len(shardNames))
	}
	table.StepDone("Sharded cluster ready: %d shard(s), %d config server(s), %d mongos on port(s) %s",
		t.Sharded, t.ConfigSrv, t.Mongos, joinPorts(mongosPorts))
	return hostPort(mongosPorts[0]), nil
}

// createDefaultUser creates the root user through the localhost exception.
func createDefaultUser(addr string) error {
	c, err := mongo.Dial(addr, 10*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = c.RunCommand("admin", mongo.D{
		{"createUser", DefaultUser},
		{"pwd", DefaultPassword},
		{"roles", mongo.A{mongo.D{{"role", "root"}, {"db", "admin"}}}},
	})
	if err != nil {
		return fmt.Errorf("create default user on %s: %w", addr, err)
	}
	return nil
}

// -------------------------------------------------------------- process mgmt

// launchReplSetMembers starts `count` data-bearing members followed by
// `arbiters` arbiter members on consecutive ports. It returns the data
// ports and the arbiter ports separately.
func launchReplSetMembers(t *Topology, st *State, rsName string, basePort, count, arbiters int, clusterRole, mongotAddr string) ([]int, []int, error) {
	var dataPorts, arbPorts []int
	port := basePort
	total := count + arbiters
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("%s-%d", rsName, i+1)
		dbpath := filepath.Join(t.dir, "data", rsName, fmt.Sprintf("rs%d", i+1), "db")
		// Annotates the stage the caller started, if any.
		table.Detail("member %d/%d on port %d", i+1, total, port)
		if _, err := launchMongod(t, st, name, port, dbpath, rsName, clusterRole, mongotAddr); err != nil {
			return nil, nil, err
		}
		dataPorts = append(dataPorts, port)
		port++
	}
	for i := 0; i < arbiters; i++ {
		name := fmt.Sprintf("%s-arb%d", rsName, i+1)
		dbpath := filepath.Join(t.dir, "data", rsName, fmt.Sprintf("arb%d", i+1), "db")
		table.Detail("arbiter %d/%d on port %d", count+i+1, total, port)
		if _, err := launchMongod(t, st, name, port, dbpath, rsName, clusterRole, mongotAddr); err != nil {
			return nil, nil, err
		}
		arbPorts = append(arbPorts, port)
		port++
	}
	table.Detail("waiting for %d member(s) to accept connections", total)
	for _, p := range append(append([]int{}, dataPorts...), arbPorts...) {
		if err := mongo.WaitReachable(hostPort(p), 60*time.Second); err != nil {
			return nil, nil, err
		}
	}
	return dataPorts, arbPorts, nil
}

// initiateReplSet runs replSetInitiate and waits for a primary; it returns
// the primary's address.
func initiateReplSet(name string, dataPorts, arbiterPorts []int, configsvr bool) (string, error) {
	members := mongo.A{}
	id := 0
	for _, p := range dataPorts {
		members = append(members, mongo.D{{"_id", id}, {"host", hostPort(p)}})
		id++
	}
	for _, p := range arbiterPorts {
		members = append(members, mongo.D{{"_id", id}, {"host", hostPort(p)}, {"arbiterOnly", true}})
		id++
	}
	cfg := mongo.D{{"_id", name}, {"members", members}}
	if configsvr {
		cfg = append(cfg, mongo.E{Key: "configsvr", Val: true})
	}

	c, err := mongo.Dial(hostPort(dataPorts[0]), 10*time.Second)
	if err != nil {
		return "", err
	}
	_, err = c.RunCommand("admin", mongo.D{{"replSetInitiate", cfg}})
	c.Close()
	if err != nil {
		if ce, ok := err.(*mongo.CommandError); ok {
			if code, _ := ce.Reply["codeName"].(string); code == "AlreadyInitialized" {
				err = nil
			}
		}
		if err != nil {
			return "", fmt.Errorf("replSetInitiate %s: %w", name, err)
		}
	}

	// Only data-bearing members can become primary.
	addrs := make([]string, len(dataPorts))
	for i, p := range dataPorts {
		addrs[i] = hostPort(p)
	}
	return mongo.WaitForPrimary(addrs, 120*time.Second)
}

func launchMongod(t *Topology, st *State, name string, port int, dbpath, replSet, clusterRole, mongotAddr string) (*Node, error) {
	if err := os.MkdirAll(dbpath, 0o755); err != nil {
		return nil, err
	}
	instDir := filepath.Dir(dbpath)
	logPath := filepath.Join(instDir, "mongod.log")
	confPath := filepath.Join(instDir, "mongod.conf")

	conf := mongodConf(t, name, dbpath, logPath, port, replSet, clusterRole, mongotAddr)
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		return nil, err
	}

	args := append([]string{"--config", confPath}, t.ExtraArgs...)
	return spawn(t.BinDir, st, "mongod", name, port, dbpath, logPath, confPath, args, t.Verbose)
}

func launchMongos(t *Topology, st *State, name string, port int, configDB, mongotAddr string) (*Node, error) {
	instDir := filepath.Join(t.dir, "data", name)
	if err := os.MkdirAll(instDir, 0o755); err != nil {
		return nil, err
	}
	logPath := filepath.Join(instDir, "mongos.log")
	confPath := filepath.Join(instDir, "mongos.conf")

	conf := mongosConf(t, name, logPath, port, configDB, mongotAddr)
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		return nil, err
	}

	args := []string{"--config", confPath}
	return spawn(t.BinDir, st, "mongos", name, port, "", logPath, confPath, args, t.Verbose)
}

// spawn runs the binary with a config that forks, and records the forked
// child's PID.
func spawn(binDir string, st *State, bin, name string, port int, dbpath, logPath, confPath string, args []string, verbose bool) (*Node, error) {
	binPath := filepath.Join(binDir, bin)
	cmd := exec.Command(binPath, args...)
	if verbose {
		fmt.Fprintf(os.Stderr, "+ %s %s\n", binPath, strings.Join(args, " "))
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s failed to start (see %s): %w", name, logPath, err)
	}

	pid, err := waitForPID(port)
	if err != nil && verbose {
		fmt.Fprintf(os.Stderr, "warning: could not determine pid for %s: %v\n", name, err)
	}

	n := &Node{Name: name, Kind: bin, Port: port, DBPath: dbpath, LogPath: logPath, ConfPath: confPath, Args: args, PID: pid}
	st.Nodes = append(st.Nodes, n)
	if st.Dir != "" {
		st.save(st.Dir) // best effort; enables cleanup of failed inits
	}
	return n, nil
}

// waitForPID finds the forked daemon's PID by looking up the process
// listening on the port (see pidListeningOn).
func waitForPID(port int) (int, error) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if pid, err := pidListeningOn(port); err == nil {
			return pid, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return 0, fmt.Errorf("no process found listening on port %d", port)
}

func hostPort(p int) string { return "127.0.0.1:" + strconv.Itoa(p) }

func joinHostPorts(ports []int) string {
	parts := make([]string, len(ports))
	for i, p := range ports {
		parts[i] = hostPort(p)
	}
	return strings.Join(parts, ",")
}

func joinPorts(ports []int) string {
	parts := make([]string, len(ports))
	for i, p := range ports {
		parts[i] = strconv.Itoa(p)
	}
	return strings.Join(parts, ", ")
}

// -------------------------------------------------- start/stop/list/destroy

// Stop sends SIGTERM (or SIGKILL if kill is true) to all nodes of the
// environment in envDir. With verbose, per-node progress is printed as
// signals are sent and as each process actually exits.
func Stop(envDir string, kill, verbose bool) error {
	st, err := LoadState(envDir)
	if err != nil {
		return fmt.Errorf("no mlaunch-go environment found in %s: %w", envDir, err)
	}
	sig, sigName := syscall.SIGTERM, "SIGTERM"
	if kill {
		sig, sigName = syscall.SIGKILL, "SIGKILL"
	}
	envName := filepath.Base(envDir)
	sp := table.StartStep("Stopping %s with %s...", envName, sigName)

	signaled := 0
	pending := map[int]bool{} // indices of nodes we signaled and wait for
	for i, n := range st.Nodes {
		pid, ok := nodeAlive(n)
		if !ok {
			if verbose {
				sp.Note("%s (port %d) not running", n.Name, n.Port)
			}
			continue
		}
		n.PID = pid
		table.Detail("signalling %s (port %d, pid %d)", n.Name, n.Port, pid)
		if err := syscall.Kill(pid, sig); err != nil {
			sp.Note("warning: could not signal %s (pid %d): %v", n.Name, pid, err)
			continue
		}
		signaled++
		pending[i] = true
	}
	st.save(envDir) // best effort: persist adopted PIDs

	if signaled == 0 {
		sp.Skip("Nothing to stop in %s: no recorded process is running", envName)
		return nil
	}

	// Wait for shutdown, reporting each process as it goes away. A large
	// environment can take a while: the detail line names what is left.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && len(pending) > 0 {
		for i := range pending {
			n := st.Nodes[i]
			if n.PID > 0 && alive(n.PID) {
				continue
			}
			if verbose {
				sp.Note("stopped %s (port %d)", n.Name, n.Port)
			}
			delete(pending, i)
		}
		if len(pending) > 0 {
			table.Detail("%d/%d stopped, waiting for %s",
				signaled-len(pending), signaled, pendingNames(st, pending))
			time.Sleep(300 * time.Millisecond)
		}
	}
	if len(pending) > 0 {
		sp.Warn("[%s] stopped %d of %d process(es); %d still running after 60s (%s)",
			envName, signaled-len(pending), signaled, len(pending), pendingNames(st, pending))
		return nil
	}
	sp.Done("[%s] stopped %d process(es)", envName, signaled)
	return nil
}

// pendingNames lists the nodes a stop is still waiting for, capped at
// three names so the progress line stays short.
func pendingNames(st *State, pending map[int]bool) string {
	names := make([]string, 0, len(pending))
	for i := range pending {
		names = append(names, st.Nodes[i].Name)
	}
	sort.Strings(names) // map order is random; keep the line stable
	if len(names) > 3 {
		return fmt.Sprintf("%s +%d more", strings.Join(names[:3], ", "), len(names)-3)
	}
	return strings.Join(names, ", ")
}

// StopNode stops a single instance identified by its listening port or its
// PID. env may be empty, in which case every environment under root is
// searched for a matching node. Only processes recorded in a state file
// are considered, so an arbitrary PID cannot be signaled by accident.
func StopNode(root, env string, port, pid int, kill, verbose bool) error {
	var dirs []string
	if env != "" {
		d, err := ResolveEnv(root, env)
		if err != nil {
			return err
		}
		dirs = []string{d}
	} else {
		var err error
		dirs, err = EnvDirs(root)
		if err != nil {
			return err
		}
	}

	sig, sigName := syscall.SIGTERM, "SIGTERM"
	if kill {
		sig, sigName = syscall.SIGKILL, "SIGKILL"
	}

	for _, d := range dirs {
		st, err := LoadState(d)
		if err != nil {
			continue
		}
		for _, n := range st.Nodes {
			curPID, running := nodeAlive(n)
			if running {
				n.PID = curPID // adopt the real daemon's PID
			}
			match := (port > 0 && n.Port == port) ||
				(pid > 0 && (n.PID == pid || (running && curPID == pid)))
			if !match {
				continue
			}
			if !running {
				table.StepSkip("[%s] %s (port %d) is not running", filepath.Base(d), n.Name, n.Port)
				return nil
			}
			sp := table.StartStep("Stopping %s (port %d) with %s...", n.Name, n.Port, sigName)
			if verbose {
				sp.Note("signalling pid %d with %s", n.PID, sigName)
			}
			if err := syscall.Kill(n.PID, sig); err != nil {
				sp.Fail("Could not signal %s (pid %d)", n.Name, n.PID)
				return fmt.Errorf("could not signal %s (pid %d): %w", n.Name, n.PID, err)
			}
			st.save(d) // best effort: persist the adopted PID
			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) && alive(n.PID) {
				table.Detail("waiting for pid %d to exit", n.PID)
				time.Sleep(300 * time.Millisecond)
			}
			if alive(n.PID) {
				sp.Fail("%s (pid %d) is still running after 60s", n.Name, n.PID)
				return fmt.Errorf("%s (pid %d) is still running after 60s", n.Name, n.PID)
			}
			sp.Done("[%s] stopped %s (port %d, pid %d)", filepath.Base(d), n.Name, n.Port, n.PID)
			return nil
		}
	}
	if pid > 0 {
		return fmt.Errorf("no instance with pid %d found in any environment under %s", pid, root)
	}
	return fmt.Errorf("no instance on port %d found in any environment under %s", port, root)
}

// StartNode relaunches a single stopped instance identified by its port.
// env may be empty, in which case every environment under root is
// searched. Note that since MongoDB 8.x a shardsvr mongod started with
// 'fork: true' blocks until it can reach the config servers, so starting a
// lone shard member of an otherwise stopped sharded cluster will hang;
// bring up the whole environment with 'start --env' in that case.
func StartNode(root, env string, port int, verbose bool) error {
	var dirs []string
	if env != "" {
		d, err := ResolveEnv(root, env)
		if err != nil {
			return err
		}
		dirs = []string{d}
	} else {
		var err error
		dirs, err = EnvDirs(root)
		if err != nil {
			return err
		}
	}

	for _, d := range dirs {
		st, err := LoadState(d)
		if err != nil {
			continue
		}
		for _, n := range st.Nodes {
			if n.Port != port {
				continue
			}
			if pid, ok := nodeAlive(n); ok {
				n.PID = pid // adopt the real daemon's PID
				st.save(d)
				table.StepSkip("[%s] %s already running on port %d (pid %d)",
					filepath.Base(d), n.Name, n.Port, n.PID)
				return nil
			}
			sp := table.StartStep("Starting %s (%s) on port %d...", n.Name, n.Kind, n.Port)
			if err := launchRecorded(st, n, verbose); err != nil {
				sp.Fail("[%s] %s failed to start on port %d", filepath.Base(d), n.Name, n.Port)
				return fmt.Errorf("start %s: %w (see %s)", n.Name, err, n.LogPath)
			}
			st.save(d)
			sp.Done("[%s] started %s on port %d (pid %d)",
				filepath.Base(d), n.Name, n.Port, n.PID)
			return nil
		}
	}
	return fmt.Errorf("no instance on port %d found in any environment under %s", port, root)
}

// Destroy force-stops the environment and removes its directory. For
// orphan directories without a state file, processes are found by scanning
// the process table for mongod/mongos/mongot started with a config file
// inside the environment (see envProcess).
func Destroy(envDir string) error {
	if _, err := LoadState(envDir); err == nil {
		if err := Stop(envDir, true, false); err != nil {
			return err
		}
	} else {
		killed := killByCmdline(envDir)
		if killed > 0 {
			fmt.Printf("[%s] killed %d orphan process(es)\n", filepath.Base(envDir), killed)
		}
	}
	if err := os.RemoveAll(envDir); err != nil {
		return err
	}
	fmt.Printf("[%s] removed %s\n", filepath.Base(envDir), envDir)
	return nil
}

// killByCmdline SIGKILLs every process that envProcess attributes to the
// environment in path. Returns the number of killed processes.
func killByCmdline(path string) int {
	procs, err := listProcesses()
	if err != nil {
		return 0
	}
	self := os.Getpid()
	killed := 0
	for pid, p := range procs {
		if pid == self || !envProcess(p, path) {
			continue
		}
		if syscall.Kill(pid, syscall.SIGKILL) == nil {
			killed++
		}
	}
	if killed > 0 {
		// Give the kernel a moment to reap, so RemoveAll doesn't race
		// with open file handles.
		time.Sleep(500 * time.Millisecond)
	}
	return killed
}

// process is one entry of the process table.
type process struct {
	name    string // executable base name, e.g. "mongod"
	cmdline string // arguments joined by spaces, starting with argv[0]
}

// envProcess reports whether p is a mongod, mongos or mongot launched by
// mlaunch-go for the environment in envDir. All of them are started with
// "--config <file inside envDir>" (the mongot launcher script execs the
// bundled JVM with the arguments passed through). Matching on the
// executable as well as that option keeps unrelated processes that merely
// mention the path, such as a shell running 'mlaunch-go destroy --dir
// <envDir>' or an editor with a mongod.conf open, out of the sweep.
func envProcess(p process, envDir string) bool {
	switch p.name {
	case "mongod", "mongos", "java":
	default:
		return false
	}
	return strings.Contains(p.cmdline, " --config "+filepath.Clean(envDir)+string(os.PathSeparator))
}

// Start relaunches all nodes recorded in the environment's state file and
// refreshes their PIDs. Order matters for sharded clusters: since MongoDB
// 8.x a shardsvr mongod started with 'fork: true' does not signal readiness
// until it can reach the config servers, so config servers must come up
// first, then the shards, then mongos.
func Start(envDir string, verbose bool) error {
	st, err := LoadState(envDir)
	if err != nil {
		return fmt.Errorf("no mlaunch-go environment found in %s: %w", envDir, err)
	}

	// isConfigSvr detects CSRS members from their generated config file,
	// which works for state files written by any version of the tool.
	isConfigSvr := func(n *Node) bool {
		data, err := os.ReadFile(n.ConfPath)
		return err == nil && strings.Contains(string(data), "clusterRole: configsvr")
	}

	// relaunch brings up one group of nodes as a single reported stage.
	// Groups that match nothing (mongos in a replica set, mongot without
	// --percona-search) are silent.
	relaunch := func(word string, match func(*Node) bool) error {
		var group []*Node
		for _, n := range st.Nodes {
			if match(n) {
				group = append(group, n)
			}
		}
		if len(group) == 0 {
			return nil
		}

		sp := table.StartStep("Starting %d %s...", len(group), pluralOf(len(group), word))
		var started, running int
		var startedPorts []int
		for i, n := range group {
			table.Detail("%s on port %d (%d/%d)", n.Name, n.Port, i+1, len(group))
			if pid, ok := nodeAlive(n); ok {
				if pid != n.PID {
					n.PID = pid // adopt the real daemon's PID
				}
				running++
				continue
			}
			if verbose {
				sp.Note("starting %s (%s) on port %d", n.Name, n.Kind, n.Port)
			}
			if err := launchRecorded(st, n, verbose); err != nil {
				sp.Fail("%s failed to start on port %d", n.Name, n.Port)
				return fmt.Errorf("restart %s: %w (see %s)", n.Name, err, n.LogPath)
			}
			st.save(envDir) // best effort, keeps PIDs fresh if a later node blocks
			started++
			startedPorts = append(startedPorts, n.Port)
		}

		switch {
		case started == 0:
			sp.Skip("All %d %s already running", running, pluralOf(running, word))
		case running == 0:
			sp.Done("Started %d %s on port(s) %s", started, pluralOf(started, word), joinPorts(startedPorts))
		default:
			sp.Done("Started %d %s on port(s) %s (%d already running)",
				started, pluralOf(started, word), joinPorts(startedPorts), running)
		}
		return nil
	}

	groups := []struct {
		word  string
		match func(*Node) bool
	}{
		{"config server", func(n *Node) bool { return n.Kind == "mongod" && isConfigSvr(n) }},
		{"mongod process", func(n *Node) bool { return n.Kind == "mongod" && !isConfigSvr(n) }},
		// mongos before mongot: a sharded mongot contacts its router
		// during bootstrap and crashes if it is not reachable. mongod
		// does not need mongot at startup, so nothing is lost by
		// starting search last.
		{"mongos router", func(n *Node) bool { return n.Kind == "mongos" }},
		{"mongot process", func(n *Node) bool { return n.Kind == "mongot" }},
	}
	for _, g := range groups {
		if err := relaunch(g.word, g.match); err != nil {
			return err
		}
	}
	if err := st.save(envDir); err != nil {
		return err
	}

	fmt.Println(table.Rule(72))
	if port := connectPort(st); port > 0 {
		table.Ready("Environment %s is up! Connect via: %s",
			filepath.Base(envDir), table.Cyan(connString(st, hostPort(port))))
	} else {
		table.Ready("Environment %s is up (%d process(es))",
			filepath.Base(envDir), len(st.Nodes))
	}
	return nil
}

// connectPort picks the port a client should connect to: the first
// running router for a sharded cluster, the current primary for a replica
// set, the instance itself otherwise. It returns 0 when nothing suitable
// is running.
func connectPort(st *State) int {
	port := 0
	for _, n := range st.Nodes {
		pid, ok := nodeAlive(n)
		if !ok {
			continue
		}
		n.PID = pid
		switch st.Topology {
		case "sharded":
			if n.Kind == "mongos" && port == 0 {
				return n.Port
			}
		case "replicaset":
			if nodeRole(n) == "primary" {
				return n.Port
			}
		default:
			if port == 0 {
				port = n.Port
			}
		}
	}
	return port
}

// pluralOf appends an "s" to word unless n is 1.
func pluralOf(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

// nodeRole queries a running node for its replica set role.
func nodeRole(n *Node) string {
	if n.Kind == "mongos" {
		return "router"
	}
	// mongot speaks gRPC, not the wire protocol: ask its health endpoint.
	if n.Kind == "mongot" {
		if mongotHealthy(n) {
			return "search"
		}
		return "starting"
	}
	c, err := mongo.Dial(hostPort(n.Port), 2*time.Second)
	if err != nil {
		return "-"
	}
	defer c.Close()
	reply, err := c.RunCommand("admin", mongo.D{{"hello", 1}})
	if err != nil {
		return "-"
	}
	if _, hasSet := reply["setName"].(string); !hasSet {
		return "standalone"
	}
	if b, _ := reply["isWritablePrimary"].(bool); b {
		return "primary"
	}
	if b, _ := reply["ismaster"].(bool); b { // pre-5.0 fallback
		return "primary"
	}
	if b, _ := reply["arbiterOnly"].(bool); b {
		return "arbiter"
	}
	if b, _ := reply["secondary"].(bool); b {
		return "secondary"
	}
	return "member"
}

// colorStatus colorizes a process/environment status word.
func colorStatus(s string) string {
	switch {
	case s == "running":
		return table.Green(s)
	case s == "partial":
		return table.Yellow(s)
	case strings.Contains(s, "incomplete"):
		return table.Red(s)
	case s == "stopped":
		return table.Dim(s)
	}
	return s
}

// colorRole colorizes a replica set role.
func colorRole(r string) string {
	switch r {
	case "primary":
		return table.Bold(table.Green(r))
	case "secondary":
		return table.Cyan(r)
	case "arbiter":
		return table.Magenta(r)
	case "router":
		return table.Blue(r)
	case "search":
		return table.Bold(table.Blue(r))
	case "starting":
		return table.Yellow(r)
	case "standalone":
		return table.Cyan(r)
	case "-":
		return table.Dim(r)
	}
	return r
}

// connString builds a mongodb:// URI for the environment, targeting addr
// (primary for replica sets, a mongos for sharded clusters, the single
// node for standalones). Credentials are percent-encoded per RFC 3986, so
// special characters in the password (like '$') survive.
func connString(st *State, addr string) string {
	cred := ""
	if st.Auth {
		cred = url.UserPassword(DefaultUser, DefaultPassword).String() + "@"
	}
	uri := "mongodb://" + cred + addr + "/"
	var params []string
	if st.Topology == "replicaset" && st.ReplSet != "" {
		params = append(params, "replicaSet="+st.ReplSet)
	}
	if st.Auth {
		params = append(params, "authSource=admin")
	}
	if len(params) > 0 {
		uri += "?" + strings.Join(params, "&")
	}
	// Single quotes keep the URI copy-pasteable into a shell ('$', '&').
	return "'" + uri + "'"
}

// List prints all environments under root (or a single one when env is
// non-empty) with per-node status and replica set roles.
func List(root, env string) error {
	dirs, err := EnvDirs(root)
	if err != nil {
		return err
	}
	if env != "" {
		d, err := ResolveEnv(root, env)
		if err != nil {
			return err
		}
		dirs = []string{d}
	}
	if len(dirs) == 0 {
		orphans, _ := OrphanDirs(root)
		if len(orphans) == 0 {
			fmt.Printf("no mlaunch-go environments found under %s\n", root)
			return nil
		}
	}

	for i, d := range dirs {
		st, err := LoadState(d)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: skipping %s: %v\n", d, err)
			continue
		}
		if i > 0 {
			fmt.Println()
		}
		running := 0
		for _, n := range st.Nodes {
			if pid, ok := nodeAlive(n); ok {
				n.PID = pid
				running++
			}
		}
		overall := "stopped"
		switch {
		case running == len(st.Nodes) && len(st.Nodes) > 0:
			overall = "running"
		case running > 0:
			overall = "partial"
		}
		if !st.Complete {
			overall += ", incomplete init"
		}

		auth := ""
		if st.Auth {
			auth = table.Yellow(fmt.Sprintf(", auth: '%s' / '%s'", DefaultUser, DefaultPassword))
		}
		fmt.Printf("%s  [%s]\n", table.Bold(filepath.Base(d)), colorStatus(overall))
		fmt.Printf("topology: %s, version: %s, members: %d, base port: %d%s\n",
			table.Cyan(st.Topology), table.Cyan(st.Version), st.Members, st.BasePort, auth)

		rows := make([][]string, 0, len(st.Nodes))
		connPort := 0
		for _, n := range st.Nodes {
			status := "stopped"
			role := "-"
			if pid, ok := nodeAlive(n); ok {
				n.PID = pid
				status = "running"
				role = nodeRole(n)
				switch st.Topology {
				case "sharded":
					if n.Kind == "mongos" && connPort == 0 {
						connPort = n.Port // first running router
					}
				case "replicaset":
					if role == "primary" {
						connPort = n.Port // current primary
					}
				default:
					if connPort == 0 {
						connPort = n.Port
					}
				}
			}
			rows = append(rows, []string{
				n.Name, n.Kind, strconv.Itoa(n.Port), strconv.Itoa(n.PID),
				colorStatus(status), colorRole(role),
			})
		}
		if connPort > 0 {
			fmt.Printf("connect: %s\n", table.Cyan(connString(st, hostPort(connPort))))
		}
		table.Render(os.Stdout, []string{"NAME", "KIND", "PORT", "PID", "STATUS", "ROLE"}, rows)
	}

	// Orphan directories: no state file, typically failed inits from
	// older versions of the tool.
	if env == "" {
		orphans, _ := OrphanDirs(root)
		for _, d := range orphans {
			if len(dirs) > 0 {
				fmt.Println()
			}
			procs := countByCmdline(d)
			fmt.Printf("%s  [%s]\n", table.Bold(filepath.Base(d)),
				table.Red(fmt.Sprintf("orphan: no state file, %d process(es) referencing it", procs)))
			fmt.Printf("clean up with: %s\n", table.Yellow("mlaunch-go destroy --env "+filepath.Base(d)))
			dirs = append(dirs, d) // only to keep the blank-line separation correct
		}
	}
	return nil
}

// countByCmdline counts the processes envProcess attributes to the
// environment in path.
func countByCmdline(path string) int {
	procs, err := listProcesses()
	if err != nil {
		return 0
	}
	self := os.Getpid()
	count := 0
	for pid, p := range procs {
		if pid != self && envProcess(p, path) {
			count++
		}
	}
	return count
}

func alive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// nodeAlive reports whether the node's process is running and returns its
// current PID. The recorded PID is preferred, but when it is stale (e.g. a
// previous 'start' was interrupted before the state file was refreshed),
// the process actually listening on the node's port is adopted instead.
func nodeAlive(n *Node) (int, bool) {
	if n.PID > 0 && alive(n.PID) {
		return n.PID, true
	}
	if pid, err := pidListeningOn(n.Port); err == nil {
		return pid, true
	}
	return 0, false
}
