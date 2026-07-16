// mlaunch-go — a modern Go rewrite of mtools' mlaunch: quickly spin up
// sandbox MongoDB deployments (standalone, replica sets, sharded clusters)
// with automatic download of MongoDB Community or Percona Server for
// MongoDB tarballs.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/PrzemekMalkowski/mlaunch-go/internal/cluster"
	"github.com/PrzemekMalkowski/mlaunch-go/internal/download"
	"github.com/PrzemekMalkowski/mlaunch-go/internal/platform"
	"github.com/PrzemekMalkowski/mlaunch-go/internal/table"
)

// version is the tool version, shown by 'mlaunch-go version'.
// version is set at build time by goreleaser via -ldflags.
var version = "dev"

const usage = `mlaunch-go — launch sandbox MongoDB environments (Go rewrite of mlaunch)

Each environment gets its own directory under the sandboxes root (--dir),
named <topology>-<members>n-<version>, e.g. replicaset-3n-8.0.26.
Default ports are derived from the version (8.0.26 -> 28026) and shifted
automatically if the range is taken by another sandbox.

Usage:
  mlaunch-go init  [options] [-- extra mongod args]   create and start a new environment
  mlaunch-go start [--dir ROOT] [--env NAME | --port PORT] [--verbose]
                                                      restart a stopped environment, or a single
                                                      instance selected by port
  mlaunch-go stop  [--dir ROOT] [--env NAME | --all | --pid PID | --port PORT] [--verbose]
                                                      stop all processes (SIGTERM), or a single
                                                      instance selected by PID or port
  mlaunch-go kill  [--dir ROOT] [--env NAME | --all | --pid PID | --port PORT] [--verbose]
                                                      force-stop (SIGKILL), same selectors as stop
  mlaunch-go destroy [--dir ROOT] [--env NAME | --all [--force]]
                                                      kill the environment(s) and remove the directory;
                                                      --all sweeps every env (asks "are you sure?",
                                                      --force skips the prompt)
  mlaunch-go list  [--dir ROOT] [--env NAME] [--no-color]
                                                      list environments, node status and roles
                                                      (colored when stdout is a TTY; respects NO_COLOR)
  mlaunch-go show-repo [--distro D] [--major M]       list versions available for download
  mlaunch-go version                                  print the mlaunch-go version

show-repo options:
  --distro NAME         "mongodb", "percona", or "all" (default "all")
  --major M             filter by major version, e.g. --major 8 or --major 8.0
  --stable yes|no       only production releases (yes) or only rc/dev builds (no)
  --download-dir DIR    where downloads are detected (default "downloads")

init options:
  --single              launch a single standalone mongod (default)
  --replicaset          launch a replica set
  --nodes N             data-bearing nodes per replica set (default 3)
  --arbiter N           arbiters per replica set (also applies to each shard; default 0)
  --name NAME           replica set name (default "replset")
  --sharded N           launch a sharded cluster with N shards
  --config N            number of config servers for --sharded (default 1)
  --mongos N            number of mongos routers for --sharded (default 1)
  --auth                enable authentication; creates user mlaunch / pa33word
  --port PORT           base port (default: derived from version)
  --dir ROOT            sandboxes root directory (default "./sandboxes")

binary resolution (in priority order):
  --binary-dir DIR      use existing mongod/mongos binaries from DIR (no download)
  --url URL             download this exact tarball URL
  --version X.Y.Z       version to download (required unless --binary-dir/--url)
  --distro NAME         "mongodb" (community, default) or "percona"
  --download-dir DIR    where tarballs are downloaded/extracted (default "downloads")

misc:
  --verbose             print launched commands and download probing
  --dry-run             resolve the download URL and print it, then exit

Examples:
  mlaunch-go init --single --version 8.3.4
  mlaunch-go init --replicaset --nodes 3 --version 8.0.26
  mlaunch-go init --replicaset --nodes 2 --arbiter 1 --version 8.0.26
  mlaunch-go init --replicaset --auth --version 8.0.26
  mlaunch-go destroy --env replicaset-3n-8.0.26
  mlaunch-go init --sharded 2 --nodes 3 --version 8.0.26
  mlaunch-go init --single --distro percona --version 8.0.4-1
  mlaunch-go list
  mlaunch-go stop --env replicaset-3n-8.0.26
  mlaunch-go stop --port 28027              # stop a single member (e.g. force a failover)
  mlaunch-go start --port 28027             # bring that member back
`

// usageText colorizes the usage string when colors are enabled: the title
// and section headings are bold, subcommand names green and option flags
// yellow. With NO_COLOR (or a non-TTY stdout) it degrades to plain text.
func usageText() string {
	lines := strings.Split(usage, "\n")
	for i, line := range lines {
		switch {
		case i == 0:
			lines[i] = table.Bold(line)
		case line != "" && !strings.HasPrefix(line, " ") && strings.HasSuffix(line, ":"):
			// Section headings like "Usage:", "init options:", "Examples:".
			lines[i] = table.Bold(table.Cyan(line))
		case strings.HasPrefix(line, "  mlaunch-go "):
			// Command lines: highlight the subcommand word.
			rest := strings.TrimPrefix(line, "  mlaunch-go ")
			cmd, tail, found := strings.Cut(rest, " ")
			colored := "  mlaunch-go " + table.Green(cmd)
			if found {
				colored += " " + tail
			}
			lines[i] = colored
		case strings.HasPrefix(strings.TrimLeft(line, " "), "--"):
			// Option lines: highlight the flag token.
			trimmed := strings.TrimLeft(line, " ")
			indent := line[:len(line)-len(trimmed)]
			flagTok, tail, found := strings.Cut(trimmed, " ")
			colored := indent + table.Yellow(flagTok)
			if found {
				colored += " " + tail
			}
			lines[i] = colored
		}
	}
	return strings.Join(lines, "\n")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usageText())
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "init":
		err = cmdInit(os.Args[2:])
	case "start":
		err = cmdStart(os.Args[2:])
	case "stop":
		err = cmdStop(os.Args[2:], false)
	case "kill":
		err = cmdStop(os.Args[2:], true)
	case "destroy":
		err = cmdEnv(os.Args[2:], true, func(envDir string, _ bool) error {
			return cluster.Destroy(envDir)
		})
	case "list":
		err = cmdList(os.Args[2:])
	case "show-repo":
		err = cmdShowRepo(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Printf("mlaunch-go version %s (%s, %s/%s)\n",
			version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	case "help", "-h", "--help":
		fmt.Print(usageText())
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usageText())
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

var versionInDirRe = regexp.MustCompile(`\d+\.\d+\.\d+(?:-\d+)?`)

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usageText()) }

	single := fs.Bool("single", false, "")
	replset := fs.Bool("replicaset", false, "")
	nodes := fs.Int("nodes", 3, "")
	arbiters := fs.Int("arbiter", 0, "")
	name := fs.String("name", "replset", "")
	sharded := fs.Int("sharded", 0, "")
	configSrv := fs.Int("config", 1, "")
	mongos := fs.Int("mongos", 1, "")
	auth := fs.Bool("auth", false, "")
	port := fs.Int("port", 0, "")
	dir := fs.String("dir", "./sandboxes", "")

	binaryDir := fs.String("binary-dir", "", "")
	rawURL := fs.String("url", "", "")
	version := fs.String("version", "", "")
	distro := fs.String("distro", "mongodb", "")
	downloadDir := fs.String("download-dir", "downloads", "")

	verbose := fs.Bool("verbose", false, "")
	dryRun := fs.Bool("dry-run", false, "")

	if err := fs.Parse(args); err != nil {
		return err
	}
	extraArgs := fs.Args() // everything after "--"

	binDir := *binaryDir
	if binDir == "" || *dryRun {
		host, err := platform.Detect()
		if err != nil {
			return err
		}
		if *verbose {
			fmt.Fprintf(os.Stderr, "detected platform: %s\n", host)
		}

		opts := download.Options{
			Dist:        download.Distribution(*distro),
			Version:     *version,
			DownloadDir: *downloadDir,
			URL:         *rawURL,
			Quiet:       !*verbose,
		}

		if *dryRun {
			urls, err := download.Resolve(opts, host)
			if err != nil {
				return err
			}
			fmt.Println("candidate URLs (in probe order):")
			for _, u := range urls {
				fmt.Println("  " + u)
			}
			return nil
		}

		res, err := download.Fetch(opts, host)
		if err != nil {
			return err
		}
		fmt.Printf("using binaries from %s\n", res.BinDir)
		binDir = filepath.Join(res.BinDir, "bin")
	}

	// The version is used for the environment name and default ports.
	// When only --binary-dir/--url is given, try to infer it from the path.
	envVersion := *version
	if envVersion == "" {
		if m := versionInDirRe.FindString(binDir); m != "" {
			envVersion = m
		} else if m := versionInDirRe.FindString(*rawURL); m != "" {
			envVersion = m
		}
	}

	absRoot, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	absBin, err := filepath.Abs(binDir)
	if err != nil {
		return err
	}

	topo := &cluster.Topology{
		Single:     *single,
		ReplicaSet: *replset,
		Nodes:      *nodes,
		Arbiters:   *arbiters,
		Name:       *name,
		Sharded:    *sharded,
		ConfigSrv:  *configSrv,
		Mongos:     *mongos,
		Auth:       *auth,
		Port:       *port,
		RootDir:    absRoot,
		BinDir:     absBin,
		Version:    envVersion,
		ExtraArgs:  extraArgs,
		Verbose:    *verbose,
	}

	// The full summary (paths, ports, connection string) is printed by Init.
	_, err = cluster.Init(topo)
	return err
}

// cmdStart handles 'start'. With --port a single stopped instance is
// relaunched (searched across all environments under the root unless
// --env narrows it down); otherwise the whole environment (or all of them
// with --all) is started via the shared cmdEnv path. --pid is rejected:
// a stopped process no longer has a PID, and the one recorded in the
// state file may have been recycled by the OS, so only --port identifies
// a stopped instance reliably.
func cmdStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	dir := fs.String("dir", "./sandboxes", "sandboxes root directory")
	env := fs.String("env", "", "environment name (subdirectory of the root)")
	fs.Bool("all", false, "apply to every environment under the root")
	pid := fs.Int("pid", 0, "not supported for start; use --port")
	port := fs.Int("port", 0, "start only the instance recorded for this port")
	verbose := fs.Bool("verbose", false, "verbose output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *pid > 0 {
		return fmt.Errorf("start --pid is not supported: a stopped process has no live PID (and recorded PIDs may be recycled); identify the instance with --port instead")
	}
	if *port > 0 {
		absRoot, err := filepath.Abs(*dir)
		if err != nil {
			return err
		}
		return cluster.StartNode(absRoot, *env, *port, *verbose)
	}

	// Whole-environment path: delegate to the shared handler (re-parses
	// args, which is safe because --pid/--port are absent in this branch).
	return cmdEnv(args, false, func(envDir string, verbose bool) error {
		return cluster.Start(envDir, verbose)
	})
}

// cmdStop handles 'stop' and 'kill'. With --pid or --port a single
// instance is stopped (searched across all environments under the root
// unless --env narrows it down); otherwise the whole environment (or all
// of them with --all) is stopped via the shared cmdEnv path.
func cmdStop(args []string, kill bool) error {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	dir := fs.String("dir", "./sandboxes", "sandboxes root directory")
	env := fs.String("env", "", "environment name (subdirectory of the root)")
	all := fs.Bool("all", false, "apply to every environment under the root")
	pid := fs.Int("pid", 0, "stop only the instance with this PID")
	port := fs.Int("port", 0, "stop only the instance listening on this port")
	verbose := fs.Bool("verbose", false, "verbose output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *pid > 0 && *port > 0 {
		return fmt.Errorf("--pid and --port are mutually exclusive")
	}
	if *pid > 0 || *port > 0 {
		if *all {
			return fmt.Errorf("--all cannot be combined with --pid/--port")
		}
		absRoot, err := filepath.Abs(*dir)
		if err != nil {
			return err
		}
		return cluster.StopNode(absRoot, *env, *port, *pid, kill, *verbose)
	}

	// Whole-environment path: delegate to the shared handler (re-parses
	// args, which is safe because --pid/--port are absent in this branch).
	return cmdEnv(args, false, func(envDir string, verbose bool) error {
		return cluster.Stop(envDir, kill, verbose)
	})
}

// cmdEnv handles start/stop/kill/destroy: resolve the environment dir under
// the sandboxes root (or use --dir directly if it is itself an environment).
// With --all the action is applied to every environment under the root;
// confirmAll additionally asks for confirmation (used by destroy) unless
// --force is given.
func cmdEnv(args []string, confirmAll bool, fn func(envDir string, verbose bool) error) error {
	fs := flag.NewFlagSet("cmd", flag.ExitOnError)
	dir := fs.String("dir", "./sandboxes", "sandboxes root directory")
	env := fs.String("env", "", "environment name (subdirectory of the root)")
	all := fs.Bool("all", false, "apply to every environment under the root")
	force := fs.Bool("force", false, "with --all: skip the confirmation prompt")
	verbose := fs.Bool("verbose", false, "verbose output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	absRoot, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}

	if *all {
		if *env != "" {
			return fmt.Errorf("--env and --all are mutually exclusive")
		}
		dirs, err := cluster.EnvDirs(absRoot)
		if err != nil {
			return err
		}
		if confirmAll {
			// destroy --all also sweeps orphan directories (failed inits).
			orphans, _ := cluster.OrphanDirs(absRoot)
			dirs = append(dirs, orphans...)
		}
		if len(dirs) == 0 {
			fmt.Printf("no mlaunch-go environments found under %s\n", absRoot)
			return nil
		}
		if confirmAll && !*force {
			names := make([]string, len(dirs))
			for i, d := range dirs {
				names[i] = "  " + filepath.Base(d)
			}
			fmt.Printf("about to destroy %d environment(s) under %s:\n%s\n",
				len(dirs), absRoot, strings.Join(names, "\n"))
			fmt.Print("are you sure? [y/N] ")
			line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
			switch strings.ToLower(strings.TrimSpace(line)) {
			case "y", "yes":
			default:
				fmt.Println("aborted")
				return nil
			}
		}
		var errs []string
		for _, d := range dirs {
			if err := fn(d, *verbose); err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", filepath.Base(d), err))
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("some environments failed:\n  %s", strings.Join(errs, "\n  "))
		}
		return nil
	}

	envDir, err := cluster.ResolveEnv(absRoot, *env)
	if err != nil {
		return err
	}
	return fn(envDir, *verbose)
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	dir := fs.String("dir", "./sandboxes", "sandboxes root directory")
	env := fs.String("env", "", "only list this environment")
	noColor := fs.Bool("no-color", false, "disable ANSI colors (colors are also off when piping or with NO_COLOR set)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *noColor {
		table.DisableColor()
	}
	absRoot, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	return cluster.List(absRoot, *env)
}

func cmdShowRepo(args []string) error {
	fs := flag.NewFlagSet("show-repo", flag.ExitOnError)
	distro := fs.String("distro", "all", "mongodb | percona | all")
	major := fs.String("major", "", "major version filter, e.g. 8 or 8.0")
	stable := fs.String("stable", "", `stability filter: "yes" (production releases only) or "no" (rc/dev only)`)
	downloadDir := fs.String("download-dir", "downloads", "where downloads are detected")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch *stable {
	case "", "yes", "no":
	default:
		return fmt.Errorf(`invalid --stable value %q (want "yes" or "no")`, *stable)
	}

	host, err := platform.Detect()
	if err != nil {
		return err
	}
	local, err := download.ListLocal(*downloadDir)
	if err != nil {
		return err
	}

	var entries []download.RepoEntry
	if *distro == "all" || *distro == "mongodb" {
		mdb, err := download.ListRemoteMongoDB(host, *major)
		if err != nil {
			return err
		}
		entries = append(entries, mdb...)
	}
	if *distro == "all" || *distro == "percona" {
		psmdb, err := download.ListRemotePercona(*major)
		if err != nil {
			return err
		}
		entries = append(entries, psmdb...)
	}

	rows := make([][]string, 0, len(entries))
	for _, e := range entries {
		if *stable == "yes" && !e.Stable {
			continue
		}
		if *stable == "no" && e.Stable {
			continue
		}
		stableCol, hostBuild, downloaded := "yes", "yes", "-"
		if !e.Stable {
			stableCol = "-"
		}
		if e.Dist == download.DistMongoDB && !e.HostBuild {
			hostBuild = "-"
		}
		if local[string(e.Dist)+":"+e.Version] {
			downloaded = "yes"
		}
		rows = append(rows, []string{e.Version, string(e.Dist), stableCol, hostBuild, downloaded})
	}
	if len(rows) == 0 {
		fmt.Println("no versions matched the given filters")
		return nil
	}
	table.Render(os.Stdout, []string{"VERSION", "DISTRO", "STABLE", "HOST BUILD", "DOWNLOADED"}, rows)
	if *major == "" {
		fmt.Println("hint: narrow the list with --major, e.g. --major 8.0")
	}
	return nil
}
