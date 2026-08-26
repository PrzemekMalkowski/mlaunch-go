// Percona Search for MongoDB (mongot) support.
//
// mongot is a separate search process that replicates from mongod over a
// change stream and answers $search / $vectorSearch stages over gRPC. One
// mongot serves a whole data-bearing replica set; in a sharded cluster
// there is one per shard (config servers need none). Clients never talk to
// mongot directly, so mlaunch-go's minimal OP_MSG client is not involved:
// it only creates the replication user and then polls the HTTP health
// endpoint.
//
// Requirements enforced elsewhere in this package: Percona Server for
// MongoDB 8.3 or later, a replica set (standalone is not supported), and
// authentication (mongot authenticates with SCRAM), so --percona-search
// implies --replicaset and --auth.
package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/PrzemekMalkowski/mlaunch-go/internal/mongo"
	"github.com/PrzemekMalkowski/mlaunch-go/internal/table"
)

// DefaultSearchDiskThreshold is the disk usage fraction above which init
// warns about mongot's replication pause. mongot's own gate is stricter
// (it pauses at 90% and only resumes below 85%), but on a large volume
// 90% still leaves plenty of room, so the warning fires later by default
// and can be tuned with --search-disk-threshold.
const DefaultSearchDiskThreshold = 0.95

const (
	// SearchUser is the account mongot uses to replicate from mongod. It
	// carries the built-in searchCoordinator role, which grants read
	// access to the data plus write access to the internal
	// __mdb_internal_search database. It shares DefaultPassword with the
	// sandbox root user.
	SearchUser = "mongot"

	// searchPasswordFile is the per-environment file mongot reads its
	// password from. It must not contain a trailing newline.
	searchPasswordFile = "mongot.pwd"

	// mongotPorts is how many consecutive ports one mongot instance
	// occupies: gRPC, health check, metrics (in that order).
	mongotPorts = 3

	// mongotStartTimeout covers JVM start plus the initial connection to
	// the sync source.
	mongotStartTimeout = 180 * time.Second

	// searchMinRAMGB is the documented minimum amount of RAM per mongot
	// process.
	searchMinRAMGB = 4
)

// searchInstances returns how many mongot processes the topology needs.
func (t *Topology) searchInstances() int {
	switch {
	case !t.Search:
		return 0
	case t.Sharded > 0:
		return t.Sharded
	default:
		return 1
	}
}

// TotalPorts returns the size of the consecutive port span the topology
// needs: one port per mongod/mongos, plus mongotPorts per mongot. The
// mongot ports are allocated at the end of the span so that adding
// --percona-search does not shift the server ports.
func (t *Topology) TotalPorts() int {
	return t.TotalMembers() + t.searchInstances()*mongotPorts
}

// mongotPortsFor returns the gRPC, health and metrics ports of the i-th
// mongot instance (0 for a replica set, shard index for a sharded
// cluster). The layout is deterministic so that mongod can be pointed at
// its mongot before the mongot itself is launched: mongotHost and
// searchIndexManagementHostAndPort are startup parameters, and mongod
// tolerates the endpoint not listening yet.
func (t *Topology) mongotPortsFor(i int) (grpc, health, metrics int) {
	base := t.Port + t.TotalMembers() + i*mongotPorts
	return base, base + 1, base + 2
}

// mongotAddr returns the gRPC address of the i-th mongot, or "" when
// search is disabled (mongodConf then emits no setParameter block).
func (t *Topology) mongotAddr(i int) string {
	if !t.Search {
		return ""
	}
	grpc, _, _ := t.mongotPortsFor(i)
	return hostPort(grpc)
}

// searchCompatible reports whether the server version is 8.3 or later,
// the minimum for the $search / $vectorSearch stages mongot plugs into.
func searchCompatible(version string) bool {
	m := versionRe.FindStringSubmatch(version)
	if m == nil {
		return false
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	return major > 8 || (major == 8 && minor >= 3)
}

// writeSearchPasswordFile writes the mongot password without a trailing
// newline (which would otherwise become part of the password).
func writeSearchPasswordFile(path string) error {
	return os.WriteFile(path, []byte(DefaultPassword), 0o600)
}

// createSearchUser creates the searchCoordinator user on addr. It runs
// after createDefaultUser, so the localhost exception is already closed
// and the connection must authenticate as the sandbox root user first.
func createSearchUser(addr string) error {
	c, err := mongo.Dial(addr, 10*time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Authenticate("admin", DefaultUser, DefaultPassword); err != nil {
		return fmt.Errorf("authenticate as %q on %s: %w", DefaultUser, addr, err)
	}
	_, err = c.RunCommand("admin", mongo.D{
		{"createUser", SearchUser},
		{"pwd", DefaultPassword},
		{"roles", mongo.A{"searchCoordinator"}},
	})
	if err != nil {
		return fmt.Errorf("create search user on %s: %w", addr, err)
	}
	return nil
}

// mongotConf renders the per-instance mongot.yml. router is the address
// of a mongos and must be set exactly when the sync source is a shard: at
// bootstrap mongot asks its sync source whether the cluster is sharded and
// refuses to start if the answer disagrees with the config
// ("cluster is sharded but syncSource.router is not configured"). For a
// plain replica set it must be left empty, or the same check fails the
// other way round.
func mongotConf(name, syncSource, router, pwdFile, dataPath string, grpcPort, healthPort, metricsPort int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# mongot.yml for instance %q, generated by mlaunch-go.\n", name)
	b.WriteString("# Edit freely; changes apply on the next 'mlaunch-go start'.\n")
	b.WriteString("# Reference: https://docs.percona.com/percona-search-for-mongodb/install-mongot.html\n\n")

	b.WriteString("syncSource:\n  replicaSet:\n")
	fmt.Fprintf(&b, "    hostAndPort: %q\n", syncSource)
	b.WriteString("    scramAuth:\n")
	fmt.Fprintf(&b, "      username: %s\n", SearchUser)
	fmt.Fprintf(&b, "      passwordFile: %q\n", pwdFile)
	b.WriteString("      authSource: admin\n")
	b.WriteString("      tls:\n        enabled: false\n")
	if router != "" {
		// The router is used for catalog/topology lookups, not for
		// replication, and authenticates as the same user — which is why
		// the search user is created both on the shard and through
		// mongos (cluster users live on the config servers).
		b.WriteString("  router:\n")
		fmt.Fprintf(&b, "    hostAndPort: %q\n", router)
		b.WriteString("    scramAuth:\n")
		fmt.Fprintf(&b, "      username: %s\n", SearchUser)
		fmt.Fprintf(&b, "      passwordFile: %q\n", pwdFile)
		b.WriteString("      authSource: admin\n")
		b.WriteString("      tls:\n        enabled: false\n")
	}
	b.WriteString("  replicationReader:\n    readPreference: \"primaryPreferred\"\n\n")

	fmt.Fprintf(&b, "storage:\n  dataPath: %q\n\n", dataPath)

	b.WriteString("server:\n  grpc:\n")
	fmt.Fprintf(&b, "    address: \"127.0.0.1:%d\"\n", grpcPort)
	b.WriteString("    tls:\n      mode: \"disabled\"\n\n")

	fmt.Fprintf(&b, "metrics:\n  enabled: true\n  address: \"127.0.0.1:%d\"\n\n", metricsPort)
	fmt.Fprintf(&b, "healthCheck:\n  address: \"127.0.0.1:%d\"\n\n", healthPort)

	b.WriteString("logging:\n  verbosity: INFO\n")
	return b.String()
}

// startSearch writes the config for the i-th mongot, launches it and waits
// until it reports SERVING. syncSource is the address of the replica set
// member mongot replicates from (the primary at init time); router is a
// mongos address in a sharded cluster and "" for a plain replica set.
func startSearch(t *Topology, st *State, i int, name, syncSource, router string) (*Node, error) {
	grpcPort, healthPort, metricsPort := t.mongotPortsFor(i)

	instDir := filepath.Join(t.dir, "data", name)
	dataPath := filepath.Join(instDir, "index")
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		return nil, err
	}
	logPath := filepath.Join(instDir, "mongot.log")
	confPath := filepath.Join(instDir, "mongot.yml")

	conf := mongotConf(name, syncSource, router, filepath.Join(t.dir, searchPasswordFile),
		dataPath, grpcPort, healthPort, metricsPort)
	if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
		return nil, err
	}

	n := &Node{
		Name:        name,
		Kind:        "mongot",
		Port:        grpcPort,
		DBPath:      dataPath,
		LogPath:     logPath,
		ConfPath:    confPath,
		Args:        []string{"--config", confPath},
		HealthPort:  healthPort,
		MetricsPort: metricsPort,
	}
	if err := startMongotProcess(filepath.Join(t.SearchBinDir, "mongot"), n, t.Verbose); err != nil {
		return nil, err
	}
	st.Nodes = append(st.Nodes, n)
	if st.Dir != "" {
		st.save(st.Dir) // best effort; enables cleanup of failed inits
	}
	if err := waitMongotHealthy(n, mongotStartTimeout); err != nil {
		// Adopt the real PID anyway, so Init's teardown kills the JVM
		// rather than the launcher script.
		adoptMongotPID(n)
		return nil, err
	}
	adoptMongotPID(n)
	return n, nil
}

// startMongotProcess launches mongot. Unlike mongod/mongos it does not
// daemonize, so it is started detached in its own session (rather than run
// to completion) and its output is redirected to the instance log.
func startMongotProcess(binPath string, n *Node, verbose bool) error {
	log, err := os.OpenFile(n.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer log.Close()

	cmd := exec.Command(binPath, n.Args...)
	cmd.Dir = filepath.Dir(binPath) // the launcher resolves bin/jdk relatively
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if verbose {
		fmt.Fprintf(os.Stderr, "+ %s %s\n", binPath, strings.Join(n.Args, " "))
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%s failed to start (see %s): %w", n.Name, n.LogPath, err)
	}
	n.PID = cmd.Process.Pid
	return cmd.Process.Release()
}

// adoptMongotPID replaces the recorded PID with the process actually
// listening on the gRPC port. The launcher is a shell script; if it does
// not exec the JVM, the PID returned by Start belongs to the wrapper and
// signalling it would leave the JVM behind.
func adoptMongotPID(n *Node) {
	if pid, err := pidListeningOn(n.Port); err == nil {
		n.PID = pid
	}
}

var healthClient = &http.Client{Timeout: 2 * time.Second}

// mongotHealthy probes the HTTP health endpoint, which reports SERVING
// once mongot is ready to accept commands from mongod.
func mongotHealthy(n *Node) bool {
	if n.HealthPort == 0 {
		return false
	}
	resp, err := healthClient.Get("http://127.0.0.1:" + strconv.Itoa(n.HealthPort) + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return resp.StatusCode == http.StatusOK && strings.Contains(string(body), "SERVING")
}

// waitMongotHealthy polls until mongot reports SERVING, the process dies,
// or the deadline passes.
func waitMongotHealthy(n *Node, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	// The launcher may fork the JVM and exit before the gRPC port is
	// bound, so a vanished PID only counts as a failure once nothing has
	// claimed the port for a while either.
	gone := 0
	for time.Now().Before(end) {
		if mongotHealthy(n) {
			return nil
		}
		if _, listening := pidListeningOn(n.Port); listening != nil && n.PID > 0 && !alive(n.PID) {
			gone++
			if gone > 20 { // ~10s
				return fmt.Errorf("%s exited during startup; see %s", n.Name, n.LogPath)
			}
		} else {
			gone = 0
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("%s did not report SERVING within %s; see %s", n.Name, deadline, n.LogPath)
}

// launchRecorded (re)starts a node from its recorded state, dispatching on
// the process kind: mongod/mongos fork themselves via their config file,
// mongot has to be detached and waited for over HTTP.
func launchRecorded(st *State, n *Node, verbose bool) error {
	if n.Kind == "mongot" {
		if st.SearchBinDir == "" {
			return fmt.Errorf("no Percona Search binary directory recorded in the state file")
		}
		if err := startMongotProcess(filepath.Join(st.SearchBinDir, "mongot"), n, verbose); err != nil {
			return err
		}
		if err := waitMongotHealthy(n, mongotStartTimeout); err != nil {
			return err
		}
		adoptMongotPID(n)
		return nil
	}

	binPath := filepath.Join(st.BinDir, n.Kind)
	cmd := exec.Command(binPath, n.Args...)
	if verbose {
		fmt.Fprintf(os.Stderr, "+ %s %s\n", binPath, strings.Join(n.Args, " "))
	}
	if err := cmd.Run(); err != nil {
		return err
	}
	if pid, err := waitForPID(n.Port); err == nil {
		n.PID = pid
	}
	return nil
}

// warnSearchResources prints the memory and disk expectations of a
// search-enabled sandbox before anything is launched. The warnings are
// advisory: init continues either way.
//
// The disk check matters more than it looks. mongot pauses replication
// entirely when its data volume crosses a usage threshold, and it does so
// silently as far as clients are concerned: the health endpoint keeps
// reporting SERVING, indexes are created and initialised, and they then
// sit in PENDING forever because no initial sync is ever queued. Catching
// a full disk here turns a baffling hang into one line at startup.
func warnSearchResources(t *Topology) {
	instances := t.searchInstances()
	if instances == 0 {
		return
	}

	// t.dir is not resolved yet at preflight time, so check the sandboxes
	// root (or its nearest existing parent).
	if usage, ok := diskUsage(t.RootDir); ok && usage >= t.searchDiskThreshold() {
		fmt.Fprintln(os.Stderr, table.Yellow(fmt.Sprintf(
			"warning: %s is %.0f%% full; mongot pauses replication above its own threshold "+
				"and search indexes will then stay PENDING forever (the health endpoint still "+
				"reports SERVING). Free some space before relying on this sandbox.",
			t.RootDir, usage*100)))
	}

	needGB := instances * searchMinRAMGB

	msg := fmt.Sprintf("warning: Percona Search needs at least %d GB of RAM per mongot process; "+
		"this environment starts %d of them (~%d GB) on top of the %d server process(es)",
		searchMinRAMGB, instances, needGB, t.TotalMembers())
	if availGB, ok := availableMemoryGB(); ok {
		msg += fmt.Sprintf("; this host reports %.1f GB available", availGB)
		if availGB < float64(needGB) {
			msg += " — expect the JVM to fail to start or the host to swap"
		}
	}
	fmt.Fprintln(os.Stderr, table.Yellow(msg))
	fmt.Fprintln(os.Stderr, table.Yellow(
		"warning: Percona Search and Percona Server for MongoDB 8.3 are Technical Preview releases; use them for sandboxes only"))
}

// searchDiskThreshold is the fraction of the sandbox volume above which
// init warns. Defaults to DefaultSearchDiskThreshold.
func (t *Topology) searchDiskThreshold() float64 {
	if t.SearchDiskThreshold > 0 {
		return t.SearchDiskThreshold
	}
	return DefaultSearchDiskThreshold
}

// diskUsage returns the used fraction (0..1) of the filesystem holding
// path, or its nearest existing parent, since the environment directory
// may not exist yet when the preflight check runs.
func diskUsage(path string) (float64, bool) {
	for p := path; ; p = filepath.Dir(p) {
		var st syscall.Statfs_t
		if err := syscall.Statfs(p, &st); err == nil {
			// Match df: usage is measured against the space actually
			// usable by this process, excluding the root reserve.
			used := st.Blocks - st.Bfree
			avail := st.Bavail
			if used+avail == 0 {
				return 0, false
			}
			return float64(used) / float64(used+avail), true
		}
		if p == "/" || p == "." || filepath.Dir(p) == p {
			return 0, false
		}
	}
}

// availableMemoryGB reads MemAvailable from /proc/meminfo.
func availableMemoryGB() (float64, bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		key, val, found := strings.Cut(sc.Text(), ":")
		if !found || key != "MemAvailable" {
			continue
		}
		fields := strings.Fields(val) // e.g. "16321048 kB"
		if len(fields) == 0 {
			return 0, false
		}
		kb, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return 0, false
		}
		return kb / (1024 * 1024), true
	}
	return 0, false
}
