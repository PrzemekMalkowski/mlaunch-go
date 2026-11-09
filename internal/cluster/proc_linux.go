package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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

// listProcesses returns every visible process, keyed by PID.
func listProcesses() (map[int]process, error) {
	procDirs, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	out := map[int]process{}
	for _, pd := range procDirs {
		pid, err := strconv.Atoi(pd.Name())
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", pd.Name(), "cmdline"))
		if err != nil || len(raw) == 0 {
			continue // kernel threads have an empty command line
		}
		argv := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
		out[pid] = process{
			name:    filepath.Base(argv[0]),
			cmdline: strings.Join(argv, " "),
		}
	}
	return out, nil
}
