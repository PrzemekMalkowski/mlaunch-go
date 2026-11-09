package cluster

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// macOS has no /proc, so the process table is queried through the stock
// lsof and ps tools, which ship with every macOS install.

// pidListeningOn asks lsof for the process listening on TCP port.
func pidListeningOn(port int) (int, error) {
	out, err := exec.Command(lsofPath(), "-nP", "-a",
		"-iTCP:"+strconv.Itoa(port), "-sTCP:LISTEN", "-t").Output()
	if err != nil {
		// lsof exits 1 when nothing matches.
		return 0, fmt.Errorf("no listening socket on port %d", port)
	}
	for _, line := range strings.Fields(string(out)) {
		if pid, err := strconv.Atoi(line); err == nil {
			return pid, nil
		}
	}
	return 0, fmt.Errorf("no listening socket on port %d", port)
}

// lsofPath prefers lsof from PATH, falling back to its stock location,
// which is not on PATH in every shell.
func lsofPath() string {
	if p, err := exec.LookPath("lsof"); err == nil {
		return p
	}
	return "/usr/sbin/lsof"
}

// listProcesses returns every visible process, keyed by PID. ps prints
// the arguments joined by spaces, so the executable is queried separately
// (comm) rather than guessed from the first word, which breaks on paths
// containing spaces.
func listProcesses() (map[int]process, error) {
	cmdlines, err := psColumn("command")
	if err != nil {
		return nil, err
	}
	names, err := psColumn("comm")
	if err != nil {
		return nil, err
	}
	out := map[int]process{}
	for pid, cmdline := range cmdlines {
		out[pid] = process{name: filepath.Base(names[pid]), cmdline: cmdline}
	}
	return out, nil
}

// psColumn returns one ps output column for every process, keyed by PID.
func psColumn(column string) (map[int]string, error) {
	// -ww disables truncation of long command lines.
	out, err := exec.Command("/bin/ps", "-axww", "-o", "pid=,"+column+"=").Output()
	if err != nil {
		return nil, err
	}
	values := map[int]string{}
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		pidStr, value, found := strings.Cut(strings.TrimSpace(sc.Text()), " ")
		if !found {
			continue
		}
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			continue
		}
		values[pid] = strings.TrimSpace(value)
	}
	return values, sc.Err()
}
