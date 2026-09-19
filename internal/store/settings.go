package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

// Settings keys owned by the legacy config bridge. Admin-plane keys
// (admin_password_hash, admin_jwt_secret, admin_setup_at) land with the
// admin plane and are never written here.
const (
	keyLogLevel                 = "log_level"
	keyMaxRetries               = "max_retries"
	keyFailureThreshold         = "failure_threshold"
	keyCooldownMs               = "cooldown_ms"
	keyStreamThresholdBytes     = "stream_threshold_bytes"
	keyDialTimeoutMs            = "dial_timeout_ms"
	keyResponseHeaderTimeoutMs  = "response_header_timeout_ms"
	keyReconcileIntervalSeconds = "reconcile_interval_seconds"
)

// Defaults applied to keys absent from the database. They mirror the legacy
// YAML defaults in internal/config while the bridge exists; once the bridge
// is removed these become the only defaults.
const (
	DefaultLogLevel                 = "info"
	DefaultMaxRetries               = 2
	DefaultFailureThreshold         = 3
	DefaultCooldownMs               = 30000
	DefaultStreamThresholdBytes     = 0 // 0 = buffer everything (opt-in streaming)
	DefaultDialTimeoutMs            = 5000
	DefaultResponseHeaderTimeoutMs  = 0 // 0 = no response-header timeout
	DefaultReconcileIntervalSeconds = 0 // 0 = periodic reconcile off
)

// Settings is the typed view of the runtime settings.
type Settings struct {
	LogLevel                 string
	MaxRetries               int
	FailureThreshold         int
	CooldownMs               int64
	StreamThresholdBytes     int64
	DialTimeoutMs            int64
	ResponseHeaderTimeoutMs  int64
	ReconcileIntervalSeconds int64
}

// defaultSettings is the baseline every stored key overrides.
func defaultSettings() Settings {
	return Settings{
		LogLevel:                 DefaultLogLevel,
		MaxRetries:               DefaultMaxRetries,
		FailureThreshold:         DefaultFailureThreshold,
		CooldownMs:               DefaultCooldownMs,
		StreamThresholdBytes:     DefaultStreamThresholdBytes,
		DialTimeoutMs:            DefaultDialTimeoutMs,
		ResponseHeaderTimeoutMs:  DefaultResponseHeaderTimeoutMs,
		ReconcileIntervalSeconds: DefaultReconcileIntervalSeconds,
	}
}

// Settings reads every runtime setting, filling defaults for keys the
// database does not have yet. A stored value that no longer parses is an
// error, not a silent default: corrupt state must be noticed, not papered
// over.
func (s *Store) Settings() (Settings, error) {
	rows, err := s.db.Query(`SELECT key, value FROM settings`)
	if err != nil {
		return Settings{}, err
	}
	defer func() { _ = rows.Close() }()
	values := map[string]string{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return Settings{}, err
		}
		values[key] = value
	}
	if err := rows.Err(); err != nil {
		return Settings{}, err
	}

	settings := defaultSettings()
	if v, ok := values[keyLogLevel]; ok {
		settings.LogLevel = v
	}
	parseInt := func(key string) (int64, bool, error) {
		v, ok := values[key]
		if !ok {
			return 0, false, nil
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("setting %s = %q is not an integer", key, v)
		}
		return n, true, nil
	}
	for _, field := range []struct {
		key   string
		apply func(int64)
	}{
		{keyMaxRetries, func(n int64) { settings.MaxRetries = int(n) }},
		{keyFailureThreshold, func(n int64) { settings.FailureThreshold = int(n) }},
		{keyCooldownMs, func(n int64) { settings.CooldownMs = n }},
		{keyStreamThresholdBytes, func(n int64) { settings.StreamThresholdBytes = n }},
		{keyDialTimeoutMs, func(n int64) { settings.DialTimeoutMs = n }},
		{keyResponseHeaderTimeoutMs, func(n int64) { settings.ResponseHeaderTimeoutMs = n }},
		{keyReconcileIntervalSeconds, func(n int64) { settings.ReconcileIntervalSeconds = n }},
	} {
		n, found, err := parseInt(field.key)
		if err != nil {
			return Settings{}, err
		}
		if found {
			field.apply(n)
		}
	}
	return settings, nil
}

// setSetting writes one settings key inside the caller's transaction.
func setSetting(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(
		`INSERT INTO settings (key, value, updated_at) VALUES (?, ?, strftime('%s', 'now'))
		 ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		key, value,
	)
	return err
}

// ErrUnknownSetting reports a settings key the management API may not write.
// Admin credential keys are deliberately absent from the whitelist: they are
// never readable or writable through the runtime-setting surface.
var ErrUnknownSetting = errors.New("unknown setting")

// runtimeSettingWriters validates one runtime setting value for the
// management API. Every key Settings() reads (except the admin credentials)
// has an entry; anything else is ErrUnknownSetting.
var runtimeSettingWriters = map[string]func(string) (string, error){
	keyLogLevel: func(v string) (string, error) {
		switch v {
		case "debug", "info", "warn", "error":
			return v, nil
		}
		return "", errors.New(`log_level must be one of debug, info, warn, error`)
	},
	keyMaxRetries:               nonNegativeInt,
	keyFailureThreshold:         positiveInt,
	keyCooldownMs:               nonNegativeInt,
	keyStreamThresholdBytes:     nonNegativeInt,
	keyDialTimeoutMs:            nonNegativeInt,
	keyResponseHeaderTimeoutMs:  nonNegativeInt,
	keyReconcileIntervalSeconds: nonNegativeInt,
}

func nonNegativeInt(v string) (string, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return "", errors.New("must be a non-negative integer")
	}
	return v, nil
}

func positiveInt(v string) (string, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		return "", errors.New("must be a positive integer")
	}
	return v, nil
}

// SetSettings validates and writes runtime settings in one transaction and
// signals one change. An unknown key or invalid value aborts the whole batch.
func (s *Store) SetSettings(values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	prepared := make([][2]string, 0, len(values))
	for key, value := range values {
		write, ok := runtimeSettingWriters[key]
		if !ok {
			return fmt.Errorf("%w: %q", ErrUnknownSetting, key)
		}
		valid, err := write(value)
		if err != nil {
			return fmt.Errorf("setting %s: %w", key, err)
		}
		prepared = append(prepared, [2]string{key, valid})
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, kv := range prepared {
		if err := setSetting(tx, kv[0], kv[1]); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.notify()
	return nil
}
