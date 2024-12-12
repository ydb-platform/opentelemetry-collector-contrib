package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
	"net/url"
)

type AuthType string

const (
	authTypeAnonymous         AuthType = "anonymous"
	authTypeServiceAccountKey AuthType = "serviceAccountKey"
	authTypeUserPassword      AuthType = "userPassword"
	authTypeAccessToken       AuthType = "accessToken"
	authTypeMetadata          AuthType = "metaData"
)

var (
	errUserAndPasswordRequired = errors.New("username and password required for userPassword authentication")
	errAccessTokenRequired     = errors.New("access token required for accessToken authentication")
	unKnownAuthType            = errors.New("unknown authentication type")
	errOnlyGrpcSupport         = errors.New("only gRPC and gRPCs schemes are supported")
	errEndpointIsNotSpecified  = errors.New("endpoint is not specified")
)

type Factory struct {
	cfg    *config.Config
	logger *zap.Logger
}

func NewFactory(cfg *config.Config, logger *zap.Logger) *Factory {
	return &Factory{cfg: cfg, logger: logger}
}

func (f *Factory) buildDSN() (string, error) {
	dsn, err := url.Parse(f.cfg.Endpoint + f.cfg.Database)
	if err != nil {
		return "", fmt.Errorf("cannot parse database URL: %w", err)
	}

	if dsn.Scheme != "grpc" && dsn.Scheme != "grpcs" {
		return "", errOnlyGrpcSupport
	}

	if dsn.Host == "" || dsn.Path == "" {
		return "", errEndpointIsNotSpecified
	}

	return dsn.String(), nil
}

func (f *Factory) buildCredentials() (ydb.Option, error) {
	switch AuthType(f.cfg.AuthType) {
	case authTypeAnonymous:
		return ydb.WithAnonymousCredentials(), nil
	case authTypeUserPassword:
		if f.cfg.Username == "" || f.cfg.Password == "" {
			return nil, errUserAndPasswordRequired
		}
		return ydb.WithStaticCredentials(f.cfg.Username, string(f.cfg.Password)), nil
	case authTypeAccessToken:
		if f.cfg.AccessToken == "" {
			return nil, errAccessTokenRequired
		}
		return ydb.WithAccessTokenCredentials(string(f.cfg.AccessToken)), nil
	// TODO: Support more authentication types.
	default:
		return nil, fmt.Errorf("%w: %q", unKnownAuthType, f.cfg.AuthType)
	}
}

func (f *Factory) BuildDB() (*DB, error) {
	dsn, err := f.buildDSN()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.cfg.Timeout)
	defer cancel()

	var opts []ydb.Option
	creds, err := f.buildCredentials()
	if err != nil {
		return nil, err
	}
	opts = append(opts, creds)
	if f.cfg.CertificatePath != "" {
		cert := ydb.WithCertificatesFromFile(f.cfg.CertificatePath)
		opts = append(opts, cert)
	}

	f.logger.Debug("creating connection to database", zap.String("dsn", dsn))
	driver, err := ydb.Open(ctx, dsn, opts...)
	if err != nil {
		return nil, err
	}

	return NewDB(driver, f.logger), nil
}
