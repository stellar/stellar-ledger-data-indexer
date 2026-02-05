package internal

import (
	_ "embed"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
)

const (
	nameSpace = "LedgerDataIndexer"
	// UnboundedModeSentinel is the value used to indicate unbounded mode for start/end ledger.
	// Values <= this indicate no specific bound is set.
	// Note: This is set to 1 (not 0) to maintain backward compatibility with existing CLI behavior
	// where the default value for start/end flags is 1, and endLedger=1 means unbounded mode.
	// Ledger sequences in Stellar start at 2 (genesis ledger), so 1 is naturally an invalid ledger.
	UnboundedModeSentinel = uint32(1)
)

var (
	Logger    = log.New()
	UserAgent = "stellar-ledger-data-indexer"
)

const (
	Pubnet                     = "pubnet"
	Testnet                    = "testnet"
	adminServerReadTimeout     = 5 * time.Second
	adminServerShutdownTimeout = 5 * time.Second
)

type StellarCoreConfig struct {
	Network               string `toml:"network"`
	NetworkPassphrase     string `toml:"network_passphrase"`
	StellarCoreBinaryPath string `toml:"stellar_core_binary_path"`
	CaptiveCoreTomlPath   string `toml:"captive_core_toml_path"`
	CheckpointFrequency   uint32 `toml:"checkpoint_frequency"`
	StoragePath           string `toml:"storage_path"`
}

type RuntimeSettings struct {
	StartLedger    uint32
	EndLedger      uint32
	ConfigFilePath string
	Backfill       bool
	MetricsPort    int
}

type PostgresConfig struct {
	Host     string `toml:"host"`
	Database string `toml:"database"`
	User     string `toml:"user"`
	Port     int    `toml:"port"`
}

type Config struct {
	DataStoreConfig   datastore.DataStoreConfig `toml:"datastore_config"`
	StellarCoreConfig StellarCoreConfig         `toml:"stellar_core_config"`
	PostgresConfig    PostgresConfig            `toml:"postgres_config"`
	StartLedger       uint32
	EndLedger         uint32
	Backfill          bool
	MetricsPort       int
}

func NewConfig(settings RuntimeSettings) (*Config, error) {

	Logger.SetLevel(log.InfoLevel)
	Logger = Logger.WithField("service", nameSpace)
	config := &Config{}

	config.StartLedger = uint32(settings.StartLedger)
	config.EndLedger = uint32(settings.EndLedger)
	config.Backfill = settings.Backfill
	config.MetricsPort = settings.MetricsPort

	Logger.Infof("Requested export with start=%d, end=%d, backfill=%t", config.StartLedger, config.EndLedger, config.Backfill)

	var err error
	if err = config.processToml(settings.ConfigFilePath); err != nil {
		return nil, err
	}

	return config, nil
}

func (config *Config) processToml(tomlPath string) error {
	// Load config TOML file
	cfg, err := toml.LoadFile(tomlPath)
	if err != nil {
		return errors.Wrapf(err, "config file %v was not found", tomlPath)
	}

	// Unmarshal TOML data into the Config struct
	if err = cfg.Unmarshal(config); err != nil {
		return errors.Wrap(err, "Error unmarshalling TOML config.")
	}

	if config.StellarCoreConfig.Network == "" && (config.StellarCoreConfig.NetworkPassphrase == "" || config.StellarCoreConfig.CaptiveCoreTomlPath == "") {
		return errors.New("Invalid captive core config, the 'network' parameter must be set to pubnet or testnet or " +
			"'stellar_core_config.network_passphrase' and 'stellar_core_config.captive_core_toml_path' must be set.")
	}

	if config.StellarCoreConfig.Network != "" && (config.StellarCoreConfig.NetworkPassphrase != "" || config.StellarCoreConfig.CaptiveCoreTomlPath != "") {
		return errors.New("Invalid captive core config, either set 'network' parameter to pubnet or testnet or " +
			"set 'stellar_core_config.network_passphrase' and 'stellar_core_config.captive_core_toml_path', not both.")
	}

	// network config values are an overlay, with network preconfigured values being first if network is present
	// and then toml settings specific for passphrase, core toml file can override lastly.
	var networkPassPhrase string
	switch config.StellarCoreConfig.Network {
	case "":

	case Pubnet:
		networkPassPhrase = network.PublicNetworkPassphrase

	case Testnet:
		networkPassPhrase = network.TestNetworkPassphrase

	default:
		return errors.New("invalid captive core config, " +
			"preconfigured_network must be set to 'pubnet' or 'testnet' or network_passphrase," +
			" and captive_core_toml_path must be set")
	}

	if config.StellarCoreConfig.NetworkPassphrase == "" {
		config.StellarCoreConfig.NetworkPassphrase = networkPassPhrase
	}

	return nil
}
