// Package mongo implements a deliberately tiny MongoDB client: just enough
// OP_MSG + BSON to run administrative commands (hello, replSetInitiate,
// addShard, ...) against unauthenticated sandbox instances launched by
// mlaunch-go. It has no external dependencies.
package mongo

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

const opMsg = 2013

// Client is a single-connection MongoDB client.
type Client struct {
	conn      net.Conn
	requestID int32
	timeout   time.Duration
}

// Dial connects to addr ("host:port").
func Dial(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, timeout: timeout}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// RunCommand executes cmd against db and returns the reply document.
// The command name must be the first element of cmd.
func (c *Client) RunCommand(db string, cmd D) (M, error) {
	full := append(D{}, cmd...)
	full = append(full, E{"$db", db})

	body, err := marshal(full)
	if err != nil {
		return nil, err
	}

	c.requestID++
	msgLen := 16 + 4 + 1 + len(body) // header + flagBits + section kind + doc
	msg := make([]byte, 0, msgLen)
	msg = binary.LittleEndian.AppendUint32(msg, uint32(msgLen))
	msg = binary.LittleEndian.AppendUint32(msg, uint32(c.requestID))
	msg = binary.LittleEndian.AppendUint32(msg, 0)     // responseTo
	msg = binary.LittleEndian.AppendUint32(msg, opMsg) // opCode
	msg = binary.LittleEndian.AppendUint32(msg, 0)     // flagBits
	msg = append(msg, 0x00)                            // section kind 0
	msg = append(msg, body...)

	if err := c.conn.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return nil, err
	}
	if _, err := c.conn.Write(msg); err != nil {
		return nil, err
	}

	// Read reply header.
	hdr := make([]byte, 16)
	if _, err := io.ReadFull(c.conn, hdr); err != nil {
		return nil, err
	}
	respLen := int(binary.LittleEndian.Uint32(hdr[0:4]))
	opCode := int(binary.LittleEndian.Uint32(hdr[12:16]))
	if respLen < 16 || respLen > 48*1024*1024 {
		return nil, fmt.Errorf("invalid reply length %d", respLen)
	}
	payload := make([]byte, respLen-16)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return nil, err
	}
	if opCode != opMsg {
		return nil, fmt.Errorf("unexpected reply opcode %d", opCode)
	}
	if len(payload) < 5+1 {
		return nil, fmt.Errorf("reply too short")
	}
	// flagBits := payload[0:4]; first section starts at 4.
	if payload[4] != 0x00 {
		return nil, fmt.Errorf("unexpected reply section kind %d", payload[4])
	}
	reply, err := unmarshal(payload[5:])
	if err != nil {
		return nil, err
	}
	if ok, _ := asFloat(reply["ok"]); ok != 1 {
		return reply, &CommandError{Reply: reply}
	}
	return reply, nil
}

// CommandError is returned when the server replies with ok != 1.
type CommandError struct{ Reply M }

func (e *CommandError) Error() string {
	msg, _ := e.Reply["errmsg"].(string)
	code, _ := asFloat(e.Reply["code"])
	if msg == "" {
		return fmt.Sprintf("command failed: %v", e.Reply)
	}
	return fmt.Sprintf("command failed (code %d): %s", int(code), msg)
}

// WaitReachable polls addr until a `ping` succeeds or the deadline passes.
func WaitReachable(addr string, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		c, err := Dial(addr, 3*time.Second)
		if err == nil {
			_, err = c.RunCommand("admin", D{{"ping", 1}})
			c.Close()
			if err == nil {
				return nil
			}
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("%s did not become reachable: %v", addr, lastErr)
}

// WaitForPrimary polls the members until one of them reports being a
// writable primary.
func WaitForPrimary(addrs []string, deadline time.Duration) (string, error) {
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		for _, addr := range addrs {
			c, err := Dial(addr, 3*time.Second)
			if err != nil {
				lastErr = err
				continue
			}
			reply, err := c.RunCommand("admin", D{{"hello", 1}})
			c.Close()
			if err != nil {
				lastErr = err
				continue
			}
			if primary, _ := reply["isWritablePrimary"].(bool); primary {
				return addr, nil
			}
			if primary, _ := reply["ismaster"].(bool); primary { // pre-5.0 fallback
				return addr, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", fmt.Errorf("no primary elected before timeout: %v", lastErr)
}
