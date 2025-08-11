package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stellar/go/support/strutils"
	internal "github.com/stellar/stellar-ledger-data-indexer/internal"
)

func Execute() error {
	rootCmd := defineCommands()
	return rootCmd.Execute()
}

func defineCommands() *cobra.Command {
	var rootCmd = &cobra.Command{
		Use:   "stellar-ledger-data-indexer",
		Short: "Export indexed Stellar ledger data to postgres database",
		Run: func(cmd *cobra.Command, args []string) {
			settings := bindCliParameters(cmd.PersistentFlags().Lookup("start"),
				cmd.PersistentFlags().Lookup("end"),
				cmd.PersistentFlags().Lookup("config-file"),
				cmd.PersistentFlags().Lookup("dataset"),
			)
			config, err := internal.NewConfig(settings)
			if err != nil {
				internal.Logger.Fatal("Failed to load configuration: ", err)
			}
			internal.IndexData(*config)
		},
	}

	rootCmd.PersistentFlags().Uint32P("start", "s", 0, "Starting ledger (inclusive), must be set to a value greater than 1")
	rootCmd.PersistentFlags().Uint32P("end", "e", 0, "Ending ledger (inclusive), optional, setting to non-zero means bounded mode, "+
		"only export ledgers from 'start' up to 'end' value which must be greater than 'start' and less than the network's current ledger. "+
		"If 'end' is absent or '0' means unbounded mode, exporter will continue to run indefintely and export the latest closed ledgers from network as they are generated in real time.")
	rootCmd.PersistentFlags().String("config-file", "config.toml", "Path to the TOML config file. Defaults to 'config.toml' on runtime working directory path.")
	rootCmd.PersistentFlags().String("dataset", "transactions", "Dataset to index")
	viper.BindPFlags(rootCmd.PersistentFlags())

	return rootCmd
}

func bindCliParameters(startFlag *pflag.Flag, endFlag *pflag.Flag, configFileFlag *pflag.Flag, datasetFlag *pflag.Flag) internal.RuntimeSettings {
	settings := internal.RuntimeSettings{}

	viper.BindPFlag(startFlag.Name, startFlag)
	viper.BindEnv(startFlag.Name, strutils.KebabToConstantCase(startFlag.Name))
	settings.StartLedger = viper.GetUint32(startFlag.Name)

	viper.BindPFlag(endFlag.Name, endFlag)
	viper.BindEnv(endFlag.Name, strutils.KebabToConstantCase(endFlag.Name))
	settings.EndLedger = viper.GetUint32(endFlag.Name)

	viper.BindPFlag(configFileFlag.Name, configFileFlag)
	viper.BindEnv(configFileFlag.Name, strutils.KebabToConstantCase(configFileFlag.Name))
	settings.ConfigFilePath = viper.GetString(configFileFlag.Name)

	viper.BindPFlag(datasetFlag.Name, datasetFlag)
	viper.BindEnv(datasetFlag.Name, strutils.KebabToConstantCase(datasetFlag.Name))
	settings.Dataset = viper.GetString(datasetFlag.Name)

	return settings
}
