// Package table renders simple ASCII tables in the style of
// github.com/olekukonko/tablewriter, without the dependency.
//
// Cells may contain ANSI SGR color sequences (see color.go); column
// widths and padding are computed on the visible characters only.
package table

import (
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// visibleWidth is the rendered width of a cell, ignoring ANSI escapes.
func visibleWidth(s string) int {
	return utf8.RuneCountInString(stripANSI(s))
}

// Render writes a bordered table to w:
//
//	+-----------+--------+-------+
//	| NAME      | KIND   | PORT  |
//	+-----------+--------+-------+
//	| replset-1 | mongod | 28026 |
//	+-----------+--------+-------+
//
// Header cells are printed bold and borders dimmed when colors are enabled.
func Render(w io.Writer, headers []string, rows [][]string) {
	cols := len(headers)
	widths := make([]int, cols)
	for i, h := range headers {
		widths[i] = visibleWidth(h)
	}
	for _, r := range rows {
		for i := 0; i < cols && i < len(r); i++ {
			if l := visibleWidth(r[i]); l > widths[i] {
				widths[i] = l
			}
		}
	}

	border := func() {
		var b strings.Builder
		for _, wd := range widths {
			b.WriteString("+")
			b.WriteString(strings.Repeat("-", wd+2))
		}
		b.WriteString("+")
		fmt.Fprintln(w, Dim(b.String()))
	}

	sep := Dim("|")
	printRow := func(cells []string, bold bool) {
		var b strings.Builder
		for i, wd := range widths {
			cell := ""
			if i < len(cells) {
				cell = cells[i]
			}
			if bold {
				cell = Bold(cell)
			}
			pad := wd - visibleWidth(cell)
			b.WriteString(sep + " ")
			b.WriteString(cell)
			b.WriteString(strings.Repeat(" ", pad))
			b.WriteString(" ")
		}
		b.WriteString(sep)
		fmt.Fprintln(w, b.String())
	}

	border()
	printRow(headers, true)
	border()
	for _, r := range rows {
		printRow(r, false)
	}
	border()
}
