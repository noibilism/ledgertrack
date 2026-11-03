package cmd

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/service"
)

func TestParseRequireAuth(t *testing.T) {
	for value, expected := range map[string]bool{"": false, "  ": false, "false": false, "0": false, "true": true, "TRUE": true, "1": true, " true ": true} {
		got, err := parseRequireAuth(value)
		require.NoError(t, err, value)
		require.Equal(t, expected, got, value)
	}
	_, err := parseRequireAuth("ture")
	require.Error(t, err, "a typo must not silently disable the guard")
}

func TestValidateAuthGuard(t *testing.T) {
	good := authGuardConfig{RequireAuth: true, Enabled: true, Issuer: "https://issuer.example", CheckScopes: true, Service: "ledger"}

	t.Run("not required keeps legacy behaviour", func(t *testing.T) {
		require.NoError(t, validateAuthGuard(authGuardConfig{}))
		require.NoError(t, validateAuthGuard(authGuardConfig{Debug: true, HTTPLog: true}))
	})
	t.Run("fully configured passes", func(t *testing.T) {
		require.NoError(t, validateAuthGuard(good))
	})
	t.Run("auth disabled is refused", func(t *testing.T) {
		cfg := good
		cfg.Enabled = false
		err := validateAuthGuard(cfg)
		require.ErrorContains(t, err, "LEDGERTRACK_REQUIRE_AUTH=true")
		require.ErrorContains(t, err, "authentication is disabled")
	})
	t.Run("missing issuer is refused", func(t *testing.T) {
		cfg := good
		cfg.Issuer = " "
		require.ErrorContains(t, validateAuthGuard(cfg), "issuer")
	})
	t.Run("scopes off is refused", func(t *testing.T) {
		cfg := good
		cfg.CheckScopes = false
		require.ErrorContains(t, validateAuthGuard(cfg), "scope checking")
	})
	t.Run("missing service is refused", func(t *testing.T) {
		cfg := good
		cfg.Service = ""
		require.ErrorContains(t, validateAuthGuard(cfg), "service name")
	})
	t.Run("debug and http log are refused", func(t *testing.T) {
		cfg := good
		cfg.Debug = true
		require.ErrorContains(t, validateAuthGuard(cfg), "debug")
		cfg = good
		cfg.HTTPLog = true
		require.ErrorContains(t, validateAuthGuard(cfg), "LEDGER_HTTP_LOG")
	})
}

func newGuardTestCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "serve"}
	auth.AddFlags(cmd.Flags())
	service.AddFlags(cmd.Flags())
	return cmd
}

func TestCheckAuthGuardFromCommand(t *testing.T) {
	t.Setenv("LEDGER_HTTP_LOG", "")

	t.Run("default is backward compatible", func(t *testing.T) {
		t.Setenv(RequireAuthEnv, "")
		require.NoError(t, checkAuthGuard(newGuardTestCommand()))
	})
	t.Run("required and auth off refuses to start", func(t *testing.T) {
		t.Setenv(RequireAuthEnv, "true")
		require.Error(t, checkAuthGuard(newGuardTestCommand()))
	})
	t.Run("required and auth fully on starts", func(t *testing.T) {
		t.Setenv(RequireAuthEnv, "true")
		cmd := newGuardTestCommand()
		require.NoError(t, cmd.Flags().Set(auth.AuthEnabledFlag, "true"))
		require.NoError(t, cmd.Flags().Set(auth.AuthIssuerFlag, "https://issuer.example"))
		require.NoError(t, cmd.Flags().Set(auth.AuthCheckScopesFlag, "true"))
		require.NoError(t, cmd.Flags().Set(auth.AuthServiceFlag, "ledger"))
		require.NoError(t, checkAuthGuard(cmd))
	})
	t.Run("invalid env value refuses to start", func(t *testing.T) {
		t.Setenv(RequireAuthEnv, "ture")
		require.Error(t, checkAuthGuard(newGuardTestCommand()))
	})
}
