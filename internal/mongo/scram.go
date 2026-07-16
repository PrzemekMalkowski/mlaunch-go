package mongo

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// Authenticate performs a SCRAM-SHA-256 (RFC 7677) conversation on the
// connection for the given user in the given auth database (usually
// "admin"). Implemented with stdlib crypto only.
func (c *Client) Authenticate(authDB, user, password string) error {
	nonceRaw := make([]byte, 18)
	if _, err := rand.Read(nonceRaw); err != nil {
		return err
	}
	clientNonce := base64.StdEncoding.EncodeToString(nonceRaw)

	// RFC 5802: '=' and ',' in the username must be escaped.
	escUser := strings.NewReplacer("=", "=3D", ",", "=2C").Replace(user)
	clientFirstBare := "n=" + escUser + ",r=" + clientNonce

	reply, err := c.RunCommand(authDB, D{
		{"saslStart", 1},
		{"mechanism", "SCRAM-SHA-256"},
		{"payload", []byte("n,," + clientFirstBare)},
		{"options", D{{"skipEmptyExchange", true}}},
	})
	if err != nil {
		return fmt.Errorf("saslStart: %w", err)
	}
	convID, ok := reply["conversationId"]
	if !ok {
		return fmt.Errorf("saslStart reply missing conversationId")
	}
	serverFirstBytes, ok := reply["payload"].([]byte)
	if !ok {
		return fmt.Errorf("saslStart reply missing payload")
	}
	serverFirst := string(serverFirstBytes)

	attrs, err := parseScramAttrs(serverFirst)
	if err != nil {
		return fmt.Errorf("server-first message: %w", err)
	}
	serverNonce := attrs["r"]
	if !strings.HasPrefix(serverNonce, clientNonce) {
		return fmt.Errorf("server nonce does not extend client nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(attrs["s"])
	if err != nil {
		return fmt.Errorf("invalid salt: %w", err)
	}
	iters, err := strconv.Atoi(attrs["i"])
	if err != nil || iters < 1 {
		return fmt.Errorf("invalid iteration count %q", attrs["i"])
	}

	saltedPwd := pbkdf2SHA256([]byte(password), salt, iters, sha256.Size)
	clientKey := hmacSHA256(saltedPwd, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)

	withoutProof := "c=biws,r=" + serverNonce
	authMsg := clientFirstBare + "," + serverFirst + "," + withoutProof

	clientSig := hmacSHA256(storedKey[:], []byte(authMsg))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	clientFinal := withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)

	reply, err = c.RunCommand(authDB, D{
		{"saslContinue", 1},
		{"conversationId", convID},
		{"payload", []byte(clientFinal)},
	})
	if err != nil {
		return fmt.Errorf("authentication failed for user %q: %w", user, err)
	}

	// Verify the server signature.
	finalBytes, _ := reply["payload"].([]byte)
	finalAttrs, err := parseScramAttrs(string(finalBytes))
	if err != nil {
		return fmt.Errorf("server-final message: %w", err)
	}
	serverKey := hmacSHA256(saltedPwd, []byte("Server Key"))
	wantSig := hmacSHA256(serverKey, []byte(authMsg))
	gotSig, err := base64.StdEncoding.DecodeString(finalAttrs["v"])
	if err != nil || !bytes.Equal(wantSig, gotSig) {
		return fmt.Errorf("server signature verification failed")
	}

	// Some server versions require one more (empty) round to finish.
	if done, _ := reply["done"].(bool); !done {
		_, err = c.RunCommand(authDB, D{
			{"saslContinue", 1},
			{"conversationId", convID},
			{"payload", []byte{}},
		})
		if err != nil {
			return fmt.Errorf("final saslContinue: %w", err)
		}
	}
	return nil
}

// parseScramAttrs parses "r=...,s=...,i=..." (values may contain '=').
func parseScramAttrs(msg string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(msg, ",") {
		k, v, found := strings.Cut(part, "=")
		if !found || len(k) != 1 {
			return nil, fmt.Errorf("malformed SCRAM attribute %q", part)
		}
		out[k] = v
	}
	return out, nil
}

func hmacSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

// pbkdf2SHA256 is PBKDF2 (RFC 2898) with HMAC-SHA-256, implemented here
// because the stdlib does not provide it and this project is dependency-free.
func pbkdf2SHA256(password, salt []byte, iter, keyLen int) []byte {
	prf := hmac.New(sha256.New, password)
	numBlocks := (keyLen + sha256.Size - 1) / sha256.Size
	out := make([]byte, 0, numBlocks*sha256.Size)
	blockIdx := make([]byte, 4)

	for block := 1; block <= numBlocks; block++ {
		prf.Reset()
		prf.Write(salt)
		binary.BigEndian.PutUint32(blockIdx, uint32(block))
		prf.Write(blockIdx)
		u := prf.Sum(nil)

		t := make([]byte, len(u))
		copy(t, u)
		for i := 1; i < iter; i++ {
			prf.Reset()
			prf.Write(u)
			u = prf.Sum(nil)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}
