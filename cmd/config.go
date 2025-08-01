package cmd

import (
	_ "embed"

	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
)

const (
	Pubnet    = "pubnet"
	Testnet   = "testnet"
	UserAgent = "LedgerDataIndexer"
)

type StellarCoreConfig struct {
	Network               string   `toml:"network"`
	NetworkPassphrase     string   `toml:"network_passphrase"`
	HistoryArchiveUrls    []string `toml:"history_archive_urls"`
	StellarCoreBinaryPath string   `toml:"stellar_core_binary_path"`
	CaptiveCoreTomlPath   string   `toml:"captive_core_toml_path"`
	CheckpointFrequency   uint32   `toml:"checkpoint_frequency"`
	StoragePath           string   `toml:"storage_path"`
}

type RuntimeSettings struct {
	StartLedger    uint32
	EndLedger      uint32
	ConfigFilePath string
}

type PostgresConfig struct {
	Host     string `toml:"host"`
	Database string `toml:"database"`
	Password string `toml:"password"`
	User     string `toml:"user"`
}

type Config struct {
	DataStoreConfig   datastore.DataStoreConfig `toml:"datastore_config"`
	StellarCoreConfig StellarCoreConfig         `toml:"stellar_core_config"`
	PostgresConfig    PostgresConfig            `toml:"postgres_config"`
	StartLedger       uint32
	EndLedger         uint32
}

func NewConfig(settings RuntimeSettings) (*Config, error) {
	config := &Config{}

	config.StartLedger = uint32(settings.StartLedger)
	config.EndLedger = uint32(settings.EndLedger)

	logger.Infof("Requested export with start=%d, end=%d", config.StartLedger, config.EndLedger)

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

	if config.StellarCoreConfig.Network == "" && (len(config.StellarCoreConfig.HistoryArchiveUrls) == 0 || config.StellarCoreConfig.NetworkPassphrase == "" || config.StellarCoreConfig.CaptiveCoreTomlPath == "") {
		return errors.New("Invalid captive core config, the 'network' parameter must be set to pubnet or testnet or " +
			"'stellar_core_config.history_archive_urls' and 'stellar_core_config.network_passphrase' and 'stellar_core_config.captive_core_toml_path' must be set.")
	}

	// network config values are an overlay, with network preconfigured values being first if network is present
	// and then toml settings specific for passphrase, archiveurls, core toml file can override lastly.
	var networkPassPhrase string
	var networkArchiveUrls []string
	switch config.StellarCoreConfig.Network {
	case "":

	case Pubnet:
		networkPassPhrase = network.PublicNetworkPassphrase
		networkArchiveUrls = network.PublicNetworkhistoryArchiveURLs

	case Testnet:
		networkPassPhrase = network.TestNetworkPassphrase
		networkArchiveUrls = network.TestNetworkhistoryArchiveURLs

	default:
		return errors.New("invalid captive core config, " +
			"preconfigured_network must be set to 'pubnet' or 'testnet' or network_passphrase, history_archive_urls," +
			" and captive_core_toml_path must be set")
	}

	if config.StellarCoreConfig.NetworkPassphrase == "" {
		config.StellarCoreConfig.NetworkPassphrase = networkPassPhrase
	}

	if len(config.StellarCoreConfig.HistoryArchiveUrls) < 1 {
		config.StellarCoreConfig.HistoryArchiveUrls = networkArchiveUrls
	}

	return nil
}
