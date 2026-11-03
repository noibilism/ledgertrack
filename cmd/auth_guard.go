package cmd

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/service"
)

// RequireAuthEnv is the environment variable that switches LedgerTrack into
// "production" mode: the process refuses to start unless authentication is
// fully configured. It defaults to false for backward compatibility.
const RequireAuthEnv = "LEDGERTRACK_REQUIRE_AUTH"

// authGuardConfig is the subset of the runtime configuration that the startup
// auth guard inspects. It is split out of the cobra command so it can be unit tested.
type authGuardConfig struct {
	RequireAuth bool
	Enabled     bool
	Issuer      string
	CheckScopes bool
	Service     string
	Debug       bool
	HTTPLog     bool
}

// parseRequireAuth interprets the LEDGERTRACK_REQUIRE_AUTH value. An empty value
// means "not required". An unparsable value is an error rather than silently
// false, so that a typo such as "ture" can not disable the guard.
func parseRequireAuth(value string) (bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return false, nil
	}
	ret, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid value %q for %s: expected true or false", value, RequireAuthEnv)
	}
	return ret, nil
}

func httpLogEnabledFromEnv() bool {
	value := strings.TrimSpace(os.Getenv("LEDGER_HTTP_LOG"))
	return value == "1" || strings.EqualFold(value, "true")
}

// validateAuthGuard returns an error when RequireAuth is set and the
// configuration would leave the API reachable without a verified bearer token,
// or would expose unauthenticated debug surfaces.
func validateAuthGuard(cfg authGuardConfig) error {
	if !cfg.RequireAuth {
		return nil
	}

	var problems []string
	if !cfg.Enabled {
		problems = append(problems, "authentication is disabled: set --auth-enabled=true (env AUTH_ENABLED=true)")
	}
	if cfg.Enabled && strings.TrimSpace(cfg.Issuer) == "" {
		problems = append(problems, "no token issuer configured: set --auth-issuer (env AUTH_ISSUER)")
	}
	if cfg.Enabled && !cfg.CheckScopes {
		problems = append(problems, "scope checking is disabled, so any valid token from the issuer could write: set --auth-check-scopes=true (env AUTH_CHECK_SCOPES=true)")
	}
	if cfg.Enabled && cfg.CheckScopes && strings.TrimSpace(cfg.Service) == "" {
		problems = append(problems, "no auth service name configured, scopes would be checked as \":read\"/\":write\": set --auth-service (env AUTH_SERVICE)")
	}
	if cfg.Debug {
		problems = append(problems, "debug mode exposes unauthenticated /_/debug/pprof and request body logging: unset --debug (env DEBUG)")
	}
	if cfg.HTTPLog {
		problems = append(problems, "LEDGER_HTTP_LOG writes request and response bodies to requests.log: unset LEDGER_HTTP_LOG")
	}

	if len(problems) == 0 {
		return nil
	}

	return errors.New(RequireAuthEnv + "=true but the configuration is not safe to start:\n  - " + strings.Join(problems, "\n  - "))
}

// authGuardConfigFromCommand reads the auth related flags (already bound to their
// environment variables) and the guard environment variable.
func authGuardConfigFromCommand(cmd *cobra.Command) (authGuardConfig, error) {
	requireAuth, err := parseRequireAuth(os.Getenv(RequireAuthEnv))
	if err != nil {
		return authGuardConfig{}, err
	}

	enabled, _ := cmd.Flags().GetBool(auth.AuthEnabledFlag)
	issuer, _ := cmd.Flags().GetString(auth.AuthIssuerFlag)
	checkScopes, _ := cmd.Flags().GetBool(auth.AuthCheckScopesFlag)
	svc, _ := cmd.Flags().GetString(auth.AuthServiceFlag)

	return authGuardConfig{
		RequireAuth: requireAuth,
		Enabled:     enabled,
		Issuer:      issuer,
		CheckScopes: checkScopes,
		Service:     svc,
		Debug:       service.IsDebug(cmd),
		HTTPLog:     httpLogEnabledFromEnv(),
	}, nil
}

// checkAuthGuard is called at the start of `serve`, before any connection is opened.
func checkAuthGuard(cmd *cobra.Command) error {
	cfg, err := authGuardConfigFromCommand(cmd)
	if err != nil {
		return err
	}
	return validateAuthGuard(cfg)
}
