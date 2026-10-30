package scram

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// RFC 7677 section 3, the SCRAM-SHA-256 example (user "user", password
// "pencil"), retrieved from rfc-editor.org/rfc/rfc7677.txt on 2026-10-30. The
// server's nonce is fixed to the RFC's so every message can be compared byte for
// byte.
func TestRFC7677ExampleExchangeByteForByte(t *testing.T) {
	salt, err := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	require.NoError(t, err)
	cred, err := NewCredential("pencil", salt, 4096)
	require.NoError(t, err)
	a := &Auther{
		Lookup: func(_ context.Context, u string) (Credential, bool, error) { return cred, u == "user", nil },
		nonce:  func() string { return "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0" },
	}
	exch, err := a.BeginAuth(context.Background(), &natsmqtt5.AuthBegin{Method: Method})
	require.NoError(t, err)

	step, err := exch.Next(context.Background(), []byte("n,,n=user,r=rOprNGfwEbeRWgbNEkqO"))
	require.NoError(t, err)
	assert.False(t, step.Done)
	assert.Equal(t, "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", string(step.Data))

	step, err = exch.Next(context.Background(), []byte("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="))
	require.NoError(t, err)
	assert.True(t, step.Done)
	assert.Equal(t, "user", step.Identity)
	assert.Equal(t, "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=", string(step.Data))
}

func TestRFC7677WrongProofIsRefused(t *testing.T) {
	cred, err := NewCredential("pencil", []byte("0123456789abcdef"), 4096)
	require.NoError(t, err)
	a := &Auther{
		Lookup: func(context.Context, string) (Credential, bool, error) { return cred, true, nil },
		nonce:  func() string { return "srv" },
	}
	exch, _ := a.BeginAuth(context.Background(), &natsmqtt5.AuthBegin{Method: Method})
	_, err = exch.Next(context.Background(), []byte("n,,n=user,r=cli"))
	require.NoError(t, err)
	_, err = exch.Next(context.Background(), []byte("c=biws,r=clisrv,p="+base64.StdEncoding.EncodeToString(make([]byte, 32))))
	require.Error(t, err)
}
