package db

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/zap/zaptest"
	"testing"
)

const (
	defaultEndpoint = "grpcs://localhost:2135"
	defaultDatabase = "/local"
)

func TestDbFactory_buildDSN(t *testing.T) {
	type fields struct {
		Endpoint string
		Database string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr error
	}{
		{
			name: "valid default config",
			fields: fields{
				Endpoint: defaultEndpoint,
				Database: defaultDatabase,
			},
			want: "grpcs://localhost:2135/local",
		},
		{
			name: "valid custom config",
			fields: fields{
				Endpoint: "grpcs://127.0.0.1:2135",
				Database: "/custom",
			},
			want: "grpcs://127.0.0.1:2135/custom",
		},
		{
			name: "grpc or grpcs required",
			fields: fields{
				Endpoint: "tcp://localhost:2135",
				Database: defaultDatabase,
			},
			wantErr: errOnlyGrpcSupport,
		},
		{
			name: "host is null",
			fields: fields{
				Endpoint: "grpcs://",
				Database: defaultDatabase,
			},
			wantErr: errEndpointIsNotSpecified,
		},
		{
			name: "path is null",
			fields: fields{
				Endpoint: defaultEndpoint,
				Database: "",
			},
			wantErr: errEndpointIsNotSpecified,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			cfg := &config.Config{
				Endpoint: tt.fields.Endpoint,
				Database: tt.fields.Database,
			}

			factory := NewFactory(cfg, logger)

			got, err := factory.buildDSN()

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr, "buildDSN()")
			} else {
				assert.Equal(t, tt.want, got, "buildDSN()")
			}
		})
	}
}

func TestDbFactory_buildDSN_failedParsing(t *testing.T) {
	type fields struct {
		Endpoint string
		Database string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name:   "empty url",
			fields: fields{},
		},
		{
			name: "invalid characters",
			fields: fields{
				Endpoint: "local host:2135",
				Database: defaultDatabase,
			},
		},
		{
			name: "missing scheme",
			fields: fields{
				Endpoint: "localhost:2135",
				Database: defaultDatabase,
			},
		},
		{
			name: "missing host",
			fields: fields{
				Endpoint: ":2135",
				Database: defaultDatabase,
			},
		},
		{
			name: "missing port",
			fields: fields{
				Endpoint: "localhost",
				Database: defaultDatabase,
			},
		},
		{
			name: "invalid port",
			fields: fields{
				Endpoint: "localhost:abc",
				Database: defaultDatabase,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			cfg := &config.Config{
				Endpoint: tt.fields.Endpoint,
				Database: tt.fields.Database,
			}

			factory := NewFactory(cfg, logger)

			_, err := factory.buildDSN()

			assert.Error(t, err)
		})
	}
}

func TestDbFactory_buildCredentials(t *testing.T) {
	type secure struct {
		AuthType          string
		Username          string
		Password          string
		AccessToken       string
		ServiceAccountKey string
	}
	tests := []struct {
		name    string
		secure  secure
		want    ydb.Option
		wantErr error
	}{
		{
			name: "valid default config",
			secure: secure{
				AuthType: "anonymous",
			},
			want: ydb.WithAnonymousCredentials(),
		},
		{
			name: "valid user and password auth",
			secure: secure{
				AuthType: "userPassword",
				Username: "user",
				Password: "pass",
			},
			want: ydb.WithStaticCredentials("user", "pass"),
		},
		{
			name: "invalid user",
			secure: secure{
				AuthType: "userPassword",
				Username: "",
				Password: "pass",
			},
			wantErr: errUserAndPasswordRequired,
		},
		{
			name: "invalid password",
			secure: secure{
				AuthType: "userPassword",
				Username: "user",
				Password: "",
			},
			wantErr: errUserAndPasswordRequired,
		},
		{
			name: "valid access token auth",
			secure: secure{
				AuthType:    "accessToken",
				AccessToken: "token",
			},
			want: ydb.WithAccessTokenCredentials("token"),
		},
		{
			name: "invalid access token",
			secure: secure{
				AuthType:    "accessToken",
				AccessToken: "",
			},
			wantErr: errAccessTokenRequired,
		},
		{
			name: "unknown auth type",
			secure: secure{
				AuthType: "unknown",
			},
			wantErr: unKnownAuthType,
		},
		//TODO: Support more authentication types
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			cfg := &config.Config{
				Endpoint:    defaultEndpoint,
				Database:    defaultDatabase,
				AuthType:    tt.secure.AuthType,
				Username:    tt.secure.Username,
				Password:    configopaque.String(tt.secure.Password),
				AccessToken: configopaque.String(tt.secure.AccessToken),
			}

			factory := NewFactory(cfg, logger)

			_, err := factory.buildCredentials()

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr, "buildCredentials()")
			} else {
				//assert.Equal(t, expDriver, actDriver, "buildCredentials()")
				assert.NoError(t, err, "buildCredentials()")
				//TODO: How to check concrete ydb.Option func?
			}
		})
	}
}
