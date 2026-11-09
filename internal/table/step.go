// Progress reporting for multi-stage operations such as 'mlaunch-go init'.
//
// Each stage is one line. While it runs it carries a spinner and an
// optional dim detail (download progress, "member 2/3", ...); when it
// finishes the line is rewritten with a status marker and, for anything
// that took a while, the elapsed time:
//
//	[⠹] Downloading Percona Server for MongoDB 8.3.7-1   142 / 280 MiB  51%  9.4 MiB/s
//	[✓] Percona Server for MongoDB 8.3.7-1 downloaded    (12.4s)
//
// On a terminal the line is animated in place. When stdout is redirected,
// TERM is "dumb", or colors were turned off with DisableColor, the same
// information is printed as plain lines (start line + result line) so that
// logs and CI output stay readable.
//
// Only one step is meant to run at a time; starting a new one finishes the
// previous. All methods are safe on a nil *Step, so callers can pass a
// step around without nil checks.
package table

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
	"unsafe"
)

// out is where progress is written. Steps go to stdout, next to the
// summary they belong to.
var out io.Writer = os.Stdout

// SetOutput redirects step output (used in tests).
func SetOutput(w io.Writer) { out = w }

const (
	clearLine     = "\x1b[2K"
	spinnerPeriod = 100 * time.Millisecond
	// detailGap separates the message from its dim detail.
	detailGap = "  "
)

var (
	spinnerUnicode = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	spinnerASCII   = []string{"|", "/", "-", "\\"}
)

// marker is the [x] prefix of a stage line.
type marker struct {
	sym   string // preferred, needs UTF-8
	ascii string // fallback
	color func(string) string
}

var (
	markRun  = marker{"+", "+", Cyan}    // stage started / informational
	markOK   = marker{"✓", "v", Green}   // stage completed
	markSkip = marker{"•", "=", Blue}    // nothing to do (already present)
	markWarn = marker{"!", "!", Yellow}  // completed, but worth a look
	markFail = marker{"✗", "x", Red}     // stage failed
)

// animEnabled is true when steps may be animated in place: stdout is a
// terminal that understands ANSI. DisableColor turns it off as well, so
// --no-color yields completely escape-free output.
var animEnabled = stdoutTTY && os.Getenv("TERM") != "dumb"

// unicodeOK reports whether the locale looks like UTF-8. When no locale is
// set at all (common in containers) the fancier glyphs are used anyway.
var unicodeOK = detectUnicode()

func detectUnicode() bool {
	for _, k := range []string{"LC_ALL", "LC_CTYPE", "LANG"} {
		v := os.Getenv(k)
		if v == "" {
			continue
		}
		u := strings.ToUpper(v)
		return strings.Contains(u, "UTF-8") || strings.Contains(u, "UTF8")
	}
	return true
}

func interactive() bool { return animEnabled && colorEnabled }

// uiMu serializes access to the terminal and to the active step's fields
// (the animation goroutine reads them).
var (
	uiMu   sync.Mutex
	active *Step
)

// Step is one stage of a longer operation.
type Step struct {
	msg    string
	detail string
	start  time.Time
	frame  int
	done   bool
	stop   chan struct{}
}

// StartStep begins a stage and, on a terminal, starts animating it.
// Finish it with Done, Skip, Warn or Fail.
func StartStep(format string, a ...any) *Step {
	s := &Step{msg: fmt.Sprintf(format, a...), start: time.Now()}

	uiMu.Lock()
	if active != nil {
		// A caller forgot to finish the previous stage; close it rather
		// than leaving a dangling spinner behind.
		active.finishLocked(markOK, active.msg)
	}
	active = s
	if interactive() {
		s.stop = make(chan struct{})
		s.renderLocked()
	} else {
		fmt.Fprintln(out, prefix(markRun)+" "+s.msg)
	}
	stop := s.stop
	uiMu.Unlock()

	if stop != nil {
		go s.animate(stop)
	}
	return s
}

func (s *Step) animate(stop chan struct{}) {
	t := time.NewTicker(spinnerPeriod)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			uiMu.Lock()
			if !s.done {
				s.frame++
				s.renderLocked()
			}
			uiMu.Unlock()
		}
	}
}

// Update replaces the message of a running step (e.g. when a download
// moves from probing to transferring).
func (s *Step) Update(format string, a ...any) {
	if s == nil {
		return
	}
	msg := fmt.Sprintf(format, a...)
	uiMu.Lock()
	defer uiMu.Unlock()
	if s.done || s.msg == msg {
		return
	}
	s.msg = msg
	s.detail = ""
	if interactive() {
		s.renderLocked()
	} else {
		fmt.Fprintln(out, prefix(markRun)+" "+msg)
	}
}

// Detail sets the dim annotation shown after the message. It is only
// rendered on a terminal: it changes many times per second and would
// otherwise flood logs.
func (s *Step) Detail(format string, a ...any) {
	if s == nil {
		return
	}
	uiMu.Lock()
	defer uiMu.Unlock()
	if s.done || !interactive() {
		return
	}
	s.detail = fmt.Sprintf(format, a...)
	s.renderLocked()
}

// Note prints an indented, dim line above the running step.
func (s *Step) Note(format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	uiMu.Lock()
	defer uiMu.Unlock()
	live := s != nil && !s.done && interactive()
	if live {
		fmt.Fprint(out, "\r"+clearLine)
	}
	fmt.Fprintln(out, "    "+Dim(msg))
	if live {
		s.renderLocked()
	}
}

// Done, Skip, Warn and Fail finish the step with the matching marker. An
// empty format keeps the message the step started with.
func (s *Step) Done(format string, a ...any) { s.finish(markOK, format, a...) }
func (s *Step) Skip(format string, a ...any) { s.finish(markSkip, format, a...) }
func (s *Step) Warn(format string, a ...any) { s.finish(markWarn, format, a...) }
func (s *Step) Fail(format string, a ...any) { s.finish(markFail, format, a...) }

// Elapsed reports how long the step has been running.
func (s *Step) Elapsed() time.Duration {
	if s == nil {
		return 0
	}
	return time.Since(s.start)
}

func (s *Step) finish(m marker, format string, a ...any) {
	if s == nil {
		return
	}
	uiMu.Lock()
	msg := s.msg
	if format != "" {
		msg = fmt.Sprintf(format, a...)
	}
	s.finishLocked(m, msg)
	uiMu.Unlock()
}

// finishLocked writes the final line. uiMu must be held.
func (s *Step) finishLocked(m marker, msg string) {
	if s.done {
		return
	}
	s.done = true
	if s.stop != nil {
		close(s.stop)
		s.stop = nil
	}
	if active == s {
		active = nil
	}

	line := prefix(m) + " " + msg
	// Only annotate stages slow enough for the duration to be interesting.
	if d := time.Since(s.start); d >= time.Second {
		line += detailGap + Dim("("+shortDuration(d)+")")
	}
	if interactive() {
		fmt.Fprint(out, "\r"+clearLine)
	}
	fmt.Fprintln(out, line)
}

// renderLocked redraws the animated line. uiMu must be held and the step
// must be running on a terminal.
func (s *Step) renderLocked() {
	frames := spinnerUnicode
	if !unicodeOK {
		frames = spinnerASCII
	}
	line := Dim("[") + Cyan(frames[s.frame%len(frames)]) + Dim("]") + " " + s.msg
	if s.detail != "" {
		line += detailGap + Dim(s.detail)
	}
	// Truncate so a narrow terminal does not wrap: a wrapped line cannot
	// be erased with a single \r + clear.
	fmt.Fprint(out, "\r"+clearLine+truncateVisible(line, termWidth()-1))
}

func prefix(m marker) string {
	sym := m.sym
	if !unicodeOK {
		sym = m.ascii
	}
	return Dim("[") + m.color(sym) + Dim("]")
}

// ------------------------------------------------------------ one-shots

// StepInfo, StepDone, StepSkip, StepWarn and StepFail print a single
// finished stage line, for steps that are instantaneous. They are safe to
// call while a step is animating: the spinner is redrawn afterwards.
func StepInfo(format string, a ...any) { stepLine(markRun, format, a...) }
func StepDone(format string, a ...any) { stepLine(markOK, format, a...) }
func StepSkip(format string, a ...any) { stepLine(markSkip, format, a...) }
func StepWarn(format string, a ...any) { stepLine(markWarn, format, a...) }
func StepFail(format string, a ...any) { stepLine(markFail, format, a...) }

// Ready prints the final, highlighted success line of an operation.
func Ready(format string, a ...any) {
	msg := Bold(Green(fmt.Sprintf(format, a...)))
	stepLineRaw(markOK, msg)
}

func stepLine(m marker, format string, a ...any) {
	stepLineRaw(m, fmt.Sprintf(format, a...))
}

func stepLineRaw(m marker, msg string) {
	uiMu.Lock()
	defer uiMu.Unlock()
	live := active != nil && !active.done && interactive()
	if live {
		fmt.Fprint(out, "\r"+clearLine)
	}
	fmt.Fprintln(out, prefix(m)+" "+msg)
	if live {
		active.renderLocked()
	}
}

// Detail annotates the currently running step, if any. It lets helpers
// deep in the call stack report progress without being handed the step.
func Detail(format string, a ...any) {
	uiMu.Lock()
	s := active
	uiMu.Unlock()
	s.Detail(format, a...)
}

// Rule returns a dim horizontal separator.
func Rule(width int) string {
	ch := "─"
	if !unicodeOK {
		ch = "-"
	}
	if w := termWidth(); w > 0 && w-1 < width {
		width = w - 1
	}
	return Dim(strings.Repeat(ch, width))
}

// ------------------------------------------------------------ formatting

// HumanBytes formats a byte count with binary units, e.g. "142.3 MiB".
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 5; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// shortDuration renders a duration compactly: "3.4s", "1m12s".
func shortDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	d = d.Round(time.Second)
	return fmt.Sprintf("%dm%02ds", int(d/time.Minute), int((d%time.Minute)/time.Second))
}

// truncateVisible cuts s to max visible columns, keeping ANSI escapes
// intact and appending an ellipsis when anything was dropped.
func truncateVisible(s string, max int) string {
	if max <= 1 || visibleWidth(s) <= max {
		return s
	}
	var b strings.Builder
	width := 0
	for i := 0; i < len(s); {
		if s[i] == 0x1b {
			// Copy the whole escape sequence; it costs no columns.
			j := i + 1
			for j < len(s) && s[j] != 'm' {
				j++
			}
			if j < len(s) {
				j++
			}
			b.WriteString(s[i:j])
			i = j
			continue
		}
		if width >= max-1 {
			break
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		b.WriteRune(r)
		width++
		i += size
	}
	if unicodeOK {
		b.WriteString("…")
	} else {
		b.WriteString(">")
	}
	if colorEnabled {
		b.WriteString(sgrReset)
	}
	return b.String()
}

// termWidth returns the terminal width in columns, falling back to 80.
func termWidth() int {
	if v := os.Getenv("COLUMNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 20 {
			return n
		}
	}
	var ws struct{ row, col, xpixel, ypixel uint16 }
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, os.Stdout.Fd(),
		uintptr(syscall.TIOCGWINSZ), uintptr(unsafe.Pointer(&ws)))
	if errno == 0 && ws.col > 20 {
		return int(ws.col)
	}
	return 80
}
