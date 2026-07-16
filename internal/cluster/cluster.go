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

	envName := EnvName(t.kind(), t.TotalMembers(), t.Version)
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
	base, err := planPorts(t.RootDir, preferred, t.TotalMembers())
	if err != nil {
		return nil, err
	}
	t.Port = base
	if t.Verbose {
		fmt.Fprintf(os.Stderr, "environment %s: ports %d-%d\n", envName, base, base+t.TotalMembers()-1)
	}

	// Keyfile for internal auth (multi-process topologies only).
	if t.Auth && t.kind() != "standalone" {
		t.keyFile = filepath.Join(t.dir, "keyfile")
		if err := writeKeyFile(t.keyFile); err != nil {
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
	}
	// Persist immediately and after every spawn (see spawn()), so that a
	// failed init leaves a state file behind: 'list' shows it as
	// incomplete and 'destroy' can clean it up.
	if err := st.save(t.dir); err != nil {
		return nil, err
	}

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
	fmt.Println(table.Dim(strings.Repeat("─", 72)))
	fmt.Printf("environment %s ready in %s\n", table.Bold(envName), t.dir)
	fmt.Printf("%d process(es), ports from %d; state file: %s\n",
		len(st.Nodes), base, statePath(t.dir))
	if t.Auth {
		fmt.Printf("authentication enabled; default user: '%s' / '%s'\n", DefaultUser, DefaultPassword)
	}
	if connAddr != "" {
		fmt.Printf("connect: %s\n", table.Cyan(connString(st, connAddr)))
	}
	return st, nil
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
func mongodConf(t *Topology, name, dbpath, logPath string, port int, replSet, clusterRole string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# mongod.conf for instance %q, generated by mlaunch-go.\n", name)
	b.WriteString("# Edit freely; changes apply on the next 'mlaunch-go start'.\n")
	b.WriteString("# Reference: https://www.mongodb.com/docs/manual/reference/configuration-options/\n\n")

	fmt.Fprintf(&b, "storage:\n  dbPath: %s\n", dbpath)
	b.WriteString("  # wiredTiger:\n  #   engineConfig:\n  #     cacheSizeGB: 1\n\n")

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

	b.WriteString("# setParameter:\n#   enableFlowControl: true\n")
	return b.String()
}

// mongosConf renders the per-router mongos.conf.
func mongosConf(t *Topology, name, logPath string, port int, configDB string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# mongos.conf for instance %q, generated by mlaunch-go.\n", name)
	b.WriteString("# Edit freely; changes apply on the next 'mlaunch-go start'.\n\n")

	fmt.Fprintf(&b, "systemLog:\n  destination: file\n  path: %s\n  logAppend: true\n\n", logPath)
	b.WriteString("processManagement:\n  fork: true\n\n")
	fmt.Fprintf(&b, "net:\n  port: %d\n  bindIp: 127.0.0.1\n\n", port)
	fmt.Fprintf(&b, "sharding:\n  configDB: %s\n\n", configDB)

	if t.Auth && t.keyFile != "" {
		fmt.Fprintf(&b, "security:\n  keyFile: %s\n", t.keyFile)
	} else {
		b.WriteString("# security:\n#   keyFile: /path/to/keyfile\n")
	}
	return b.String()
}

// ---------------------------------------------------------------- topologies

func initSingle(t *Topology, st *State) (string, error) {
	n, err := launchMongod(t, st, "single", t.Port, filepath.Join(t.dir, "data", "db"), "", "")
	if err != nil {
		return "", err
	}
	if err := mongo.WaitReachable(hostPort(n.Port), 60*time.Second); err != nil {
		return "", err
	}
	if t.Auth {
		if err := createDefaultUser(hostPort(n.Port)); err != nil {
			return "", err
		}
	}
	fmt.Printf("launched standalone mongod on port %d (pid %d)\n", n.Port, n.PID)
	return hostPort(n.Port), nil
}

func initReplicaSet(t *Topology, st *State) (string, error) {
	dataPorts, arbPorts, err := launchReplSetMembers(t, st, t.Name, t.Port, t.Nodes, t.Arbiters, "")
	if err != nil {
		return "", err
	}
	primary, err := initiateReplSet(t.Name, dataPorts, arbPorts, false)
	if err != nil {
		return "", err
	}
	if t.Auth {
		if err := createDefaultUser(primary); err != nil {
			return "", err
		}
	}
	if len(arbPorts) > 0 {
		fmt.Printf("replica set %q initiated (%d data, %d arbiter); primary elected\n",
			t.Name, len(dataPorts), len(arbPorts))
	} else {
		fmt.Printf("replica set %q initiated; primary elected\n", t.Name)
	}
	return primary, nil
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
	for s := 1; s <= t.Sharded; s++ {
		rsName := fmt.Sprintf("sh%02d", s)
		dataPorts, arbPorts, err := launchReplSetMembers(t, st, rsName, port, t.Nodes, t.Arbiters, "shardsvr")
		if err != nil {
			return "", err
		}
		port += t.Nodes + t.Arbiters
		primary, err := initiateReplSet(rsName, dataPorts, arbPorts, false)
		if err != nil {
			return "", err
		}
		if t.Auth {
			if err := createDefaultUser(primary); err != nil {
				return "", fmt.Errorf("shard-local user on %s: %w", rsName, err)
			}
		}
		// Only data-bearing members go into the addShard seed list.
		shardSpecs = append(shardSpecs, rsName+"/"+joinHostPorts(dataPorts))
		fmt.Printf("shard %s ready (%s)\n", rsName, joinHostPorts(dataPorts))
	}
	if t.Auth {
		fmt.Printf("shard-local user '%s' created on every shard (for direct shard connections)\n", DefaultUser)
	}

	// 2. Config server replica set (arbiters are not supported on CSRS).
	csName := "configRepl"
	csPorts, _, err := launchReplSetMembers(t, st, csName, port, t.ConfigSrv, 0, "configsvr")
	if err != nil {
		return "", err
	}
	port += t.ConfigSrv
	if _, err := initiateReplSet(csName, csPorts, nil, true); err != nil {
		return "", err
	}
	fmt.Printf("config server replica set ready (%s)\n", joinHostPorts(csPorts))

	// 3. mongos routers.
	var mongosPorts []int
	for m := 1; m <= t.Mongos; m++ {
		n, err := launchMongos(t, st, fmt.Sprintf("mongos%d", m), port, csName+"/"+joinHostPorts(csPorts))
		if err != nil {
			return "", err
		}
		mongosPorts = append(mongosPorts, n.Port)
		port++
	}
	for _, p := range mongosPorts {
		if err := mongo.WaitReachable(hostPort(p), 90*time.Second); err != nil {
			return "", err
		}
	}

	// 4. With auth: create the cluster-wide default user via the first
	// mongos while the localhost exception is still open, then
	// authenticate for addShard.
	c, err := mongo.Dial(hostPort(mongosPorts[0]), 10*time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	if t.Auth {
		if err := createDefaultUser(hostPort(mongosPorts[0])); err != nil {
			return "", err
		}
		if err := c.Authenticate("admin", DefaultUser, DefaultPassword); err != nil {
			return "", err
		}
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
		fmt.Println("cluster-wide default write concern set to {w: 1} (shards contain arbiters)")
	}

	// 6. Register shards through the first mongos.
	for _, spec := range shardSpecs {
		if _, err := c.RunCommand("admin", mongo.D{{"addShard", spec}}); err != nil {
			return "", fmt.Errorf("addShard %s: %w", spec, err)
		}
	}
	fmt.Printf("sharded cluster ready: %d shard(s), %d config server(s), %d mongos on port(s) %s\n",
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
func launchReplSetMembers(t *Topology, st *State, rsName string, basePort, count, arbiters int, clusterRole string) ([]int, []int, error) {
	var dataPorts, arbPorts []int
	port := basePort
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("%s-%d", rsName, i+1)
		dbpath := filepath.Join(t.dir, "data", rsName, fmt.Sprintf("rs%d", i+1), "db")
		if _, err := launchMongod(t, st, name, port, dbpath, rsName, clusterRole); err != nil {
			return nil, nil, err
		}
		dataPorts = append(dataPorts, port)
		port++
	}
	for i := 0; i < arbiters; i++ {
		name := fmt.Sprintf("%s-arb%d", rsName, i+1)
		dbpath := filepath.Join(t.dir, "data", rsName, fmt.Sprintf("arb%d", i+1), "db")
		if _, err := launchMongod(t, st, name, port, dbpath, rsName, clusterRole); err != nil {
			return nil, nil, err
		}
		arbPorts = append(arbPorts, port)
		port++
	}
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

func launchMongod(t *Topology, st *State, name string, port int, dbpath, replSet, clusterRole string) (*Node, error) {
	if err := os.MkdirAll(dbpath, 0o755); err != nil {
		return nil, err
	}
	instDir := filepath.Dir(dbpath)
	logPath := filepath.Join(instDir, "mongod.log")
	confPath := filepath.Join(instDir, "mongod.conf")

	conf := mongodConf(t, name, dbpath, logPath, port, replSet, clusterRole)
	if err := os.WriteFile(confPath, []byte(conf), 0o644); err != nil {
		return nil, err
	}

	args := append([]string{"--config", confPath}, t.ExtraArgs...)
	return spawn(t.BinDir, st, "mongod", name, port, dbpath, logPath, confPath, args, t.Verbose)
}

func launchMongos(t *Topology, st *State, name string, port int, configDB string) (*Node, error) {
	instDir := filepath.Join(t.dir, "data", name)
	if err := os.MkdirAll(instDir, 0o755); err != nil {
		return nil, err
	}
	logPath := filepath.Join(instDir, "mongos.log")
	confPath := filepath.Join(instDir, "mongos.conf")

	conf := mongosConf(t, name, logPath, port, configDB)
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
// listening on the port via /proc.
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

// pidListeningOn scans /proc to find the process listening on TCP port.
func pidListeningOn(port int) (int, error) {
	inode, err := listeningInode(port)
	if err != nil {
		return 0, err
	}
	procDirs, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	target := "socket:[" + inode + "]"
	for _, pd := range procDirs {
		pid, err := strconv.Atoi(pd.Name())
		if err != nil {
			continue
		}
		fdDir := filepath.Join("/proc", pd.Name(), "fd")
		fds, err := os.ReadDir(fdDir)
		if err != nil {
			continue
		}
		for _, fd := range fds {
			link, err := os.Readlink(filepath.Join(fdDir, fd.Name()))
			if err == nil && link == target {
				return pid, nil
			}
		}
	}
	return 0, fmt.Errorf("owner of socket inode %s not found", inode)
}

// listeningInode parses /proc/net/tcp for a LISTEN socket on port.
func listeningInode(port int) (string, error) {
	for _, file := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		hexPort := fmt.Sprintf("%04X", port)
		for _, line := range strings.Split(string(data), "\n")[1:] {
			fields := strings.Fields(line)
			if len(fields) < 10 {
				continue
			}
			local := fields[1] // e.g. 0100007F:69B9
			st := fields[3]    // 0A = LISTEN
			if st != "0A" {
				continue
			}
			if strings.HasSuffix(local, ":"+hexPort) {
				return fields[9], nil
			}
		}
	}
	return "", fmt.Errorf("no listening socket on port %d", port)
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
	stopped := 0
	pending := map[int]bool{} // indices of nodes we signaled and wait for
	for i, n := range st.Nodes {
		pid, ok := nodeAlive(n)
		if !ok {
			if verbose {
				fmt.Printf("%s (port %d) not running\n", n.Name, n.Port)
			}
			continue
		}
		n.PID = pid
		if verbose {
			fmt.Printf("stopping %s (port %d, pid %d) with %s ...\n", n.Name, n.Port, pid, sigName)
		}
		if err := syscall.Kill(pid, sig); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not signal %s (pid %d): %v\n", n.Name, pid, err)
			continue
		}
		stopped++
		pending[i] = true
	}
	st.save(envDir) // best effort: persist adopted PIDs
	// Wait for shutdown, reporting each process as it goes away.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && len(pending) > 0 {
		for i := range pending {
			n := st.Nodes[i]
			if n.PID > 0 && alive(n.PID) {
				continue
			}
			if verbose {
				fmt.Printf("stopped %s (port %d)\n", n.Name, n.Port)
			}
			delete(pending, i)
		}
		if len(pending) > 0 {
			time.Sleep(300 * time.Millisecond)
		}
	}
	if len(pending) > 0 {
		fmt.Fprintf(os.Stderr, "warning: %d process(es) still running after 60s\n", len(pending))
	}
	fmt.Printf("[%s] stopped %d process(es)\n", filepath.Base(envDir), stopped)
	return nil
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
				fmt.Printf("[%s] %s (port %d) is not running\n", filepath.Base(d), n.Name, n.Port)
				return nil
			}
			if verbose {
				fmt.Printf("stopping %s (port %d, pid %d) with %s ...\n", n.Name, n.Port, n.PID, sigName)
			}
			if err := syscall.Kill(n.PID, sig); err != nil {
				return fmt.Errorf("could not signal %s (pid %d): %w", n.Name, n.PID, err)
			}
			st.save(d) // best effort: persist the adopted PID
			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) && alive(n.PID) {
				time.Sleep(300 * time.Millisecond)
			}
			if alive(n.PID) {
				return fmt.Errorf("%s (pid %d) is still running after 60s", n.Name, n.PID)
			}
			fmt.Printf("[%s] stopped %s (port %d, pid %d)\n", filepath.Base(d), n.Name, n.Port, n.PID)
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
				fmt.Printf("[%s] %s already running on port %d (pid %d)\n",
					filepath.Base(d), n.Name, n.Port, n.PID)
				return nil
			}
			binPath := filepath.Join(st.BinDir, n.Kind)
			if verbose {
				fmt.Printf("starting %s (%s) on port %d ...\n", n.Name, n.Kind, n.Port)
				fmt.Fprintf(os.Stderr, "+ %s %s\n", binPath, strings.Join(n.Args, " "))
			}
			cmd := exec.Command(binPath, n.Args...)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("start %s: %w (see %s)", n.Name, err, n.LogPath)
			}
			if pid, err := waitForPID(n.Port); err == nil {
				n.PID = pid
			}
			st.save(d)
			fmt.Printf("[%s] started %s on port %d (pid %d)\n",
				filepath.Base(d), n.Name, n.Port, n.PID)
			return nil
		}
	}
	return fmt.Errorf("no instance on port %d found in any environment under %s", port, root)
}

// Destroy force-stops the environment and removes its directory. For
// orphan directories without a state file, processes are found by scanning
// /proc for command lines that reference the environment path.
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

// killByCmdline SIGKILLs every process whose command line contains path
// (mongod/mongos launched by mlaunch-go always reference their config file
// inside the environment directory). Returns the number of killed processes.
func killByCmdline(path string) int {
	procDirs, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	self := os.Getpid()
	killed := 0
	for _, pd := range procDirs {
		pid, err := strconv.Atoi(pd.Name())
		if err != nil || pid == self {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", pd.Name(), "cmdline"))
		if err != nil {
			continue
		}
		cmdline := strings.ReplaceAll(string(raw), "\x00", " ")
		if !strings.Contains(cmdline, path) {
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

	relaunch := func(match func(*Node) bool) error {
		for _, n := range st.Nodes {
			if !match(n) {
				continue
			}
			if pid, ok := nodeAlive(n); ok {
				if pid != n.PID {
					n.PID = pid // adopt the real daemon's PID
				}
				fmt.Printf("%s already running (pid %d)\n", n.Name, n.PID)
				continue
			}
			binPath := filepath.Join(st.BinDir, n.Kind)
			cmd := exec.Command(binPath, n.Args...)
			if verbose {
				fmt.Printf("starting %s (%s) on port %d ...\n", n.Name, n.Kind, n.Port)
				fmt.Fprintf(os.Stderr, "+ %s %s\n", binPath, strings.Join(n.Args, " "))
			}
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("restart %s: %w (see %s)", n.Name, err, n.LogPath)
			}
			pid, err := waitForPID(n.Port)
			if err == nil {
				n.PID = pid
			}
			st.save(envDir) // best effort, keeps PIDs fresh if a later node blocks
			fmt.Printf("started %s on port %d (pid %d)\n", n.Name, n.Port, n.PID)
		}
		return nil
	}

	for _, group := range []func(*Node) bool{
		func(n *Node) bool { return n.Kind == "mongod" && isConfigSvr(n) },
		func(n *Node) bool { return n.Kind == "mongod" && !isConfigSvr(n) },
		func(n *Node) bool { return n.Kind == "mongos" },
	} {
		if err := relaunch(group); err != nil {
			return err
		}
	}
	return st.save(envDir)
}

// nodeRole queries a running node for its replica set role.
func nodeRole(n *Node) string {
	if n.Kind == "mongos" {
		return "router"
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

// countByCmdline counts processes whose command line references path.
func countByCmdline(path string) int {
	procDirs, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	self := os.Getpid()
	count := 0
	for _, pd := range procDirs {
		pid, err := strconv.Atoi(pd.Name())
		if err != nil || pid == self {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", pd.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if strings.Contains(strings.ReplaceAll(string(raw), "\x00", " "), path) {
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
