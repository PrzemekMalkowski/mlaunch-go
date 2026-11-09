// ANSI color support for terminal output.
//
// Colors are enabled only when stdout is a terminal, the NO_COLOR
// convention (https://no-color.org) is not set, and TERM is not "dumb".
// All helpers degrade to plain strings when colors are disabled, so
// callers never need to branch.
package table

import (
	"fmt"
	"os"
	"regexp"
)

// stdoutTTY and colorEnabled are decided once at startup.
var (
	stdoutTTY    = isCharDevice(os.Stdout)
	colorEnabled = detectColor()
)

func isCharDevice(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func detectColor() bool {
	if _, set := os.LookupEnv("NO_COLOR"); set {
		return false
	}
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	return stdoutTTY
}

// ColorEnabled reports whether ANSI colors are active for stdout.
func ColorEnabled() bool { return colorEnabled }

// IsTerminal reports whether stdout is a terminal.
func IsTerminal() bool { return stdoutTTY }

// DisableColor turns colors off for the rest of the process (used by a
// --no-color flag, for example). Step animation is disabled with it, so
// the output contains no escape sequences at all (see step.go).
func DisableColor() { colorEnabled = false }

// SGR codes.
const (
	sgrReset   = "\x1b[0m"
	sgrBold    = "\x1b[1m"
	sgrDim     = "\x1b[2m"
	sgrRed     = "\x1b[31m"
	sgrGreen   = "\x1b[32m"
	sgrYellow  = "\x1b[33m"
	sgrBlue    = "\x1b[34m"
	sgrMagenta = "\x1b[35m"
	sgrCyan    = "\x1b[36m"
)

func paint(code, s string) string {
	if !colorEnabled || s == "" {
		return s
	}
	return code + s + sgrReset
}

// Basic styles.
func Bold(s string) string    { return paint(sgrBold, s) }
func Dim(s string) string     { return paint(sgrDim, s) }
func Red(s string) string     { return paint(sgrRed, s) }
func Green(s string) string   { return paint(sgrGreen, s) }
func Yellow(s string) string  { return paint(sgrYellow, s) }
func Blue(s string) string    { return paint(sgrBlue, s) }
func Magenta(s string) string { return paint(sgrMagenta, s) }
func Cyan(s string) string    { return paint(sgrCyan, s) }

// Boldf is a convenience for formatted bold text.
func Boldf(format string, a ...any) string {
	return Bold(fmt.Sprintf(format, a...))
}

// ansiRe matches SGR escape sequences so cell widths can be computed on
// the visible characters only.
var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// stripANSI removes SGR escape sequences from s.
func stripANSI(s string) string {
	return ansiRe.ReplaceAllString(s, "")
}
