package nxsugar

import (
	"os"

	flag "github.com/ogier/pflag"
)

var flagsParsed = false
var flagsEnabled = true

func parseFlags() {
	var cf string
	var pm bool
	if flagsEnabled && !flagsParsed {
		flag.CommandLine.Init(os.Args[0], flag.ContinueOnError)
		flag.StringVarP(&cf, "config", "c", "config.json", "JSON configuration file")
		flag.BoolVar(&pm, "production", false, "Enables Production mode")
		flag.Parse()
		flagsParsed = true
		if configFile == "" {
			configFile = cf
		}
		if !productionModeSet {
			productionMode = pm
			SetJSONOutput(productionMode)
		}
	}
}

func SetFlagsEnabled(t bool) {
	flagsEnabled = t
}
