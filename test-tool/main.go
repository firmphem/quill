package main

import (
	"flag"
	"os"
	"reflect"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	testSuiteDir      string
	configFile        string
	includeTestsRegex string
	cfg               config
	registry          = map[string]reflect.Value{}
)

// -----------------------------------------------------------------------------
func init_logging() {
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

// -----------------------------------------------------------------------------
func init() {
	flag.StringVar(&configFile, "config", "", "path to config yaml (contains kafka + database)")
	flag.StringVar(&testSuiteDir, "test-suite-dir", "", "directory containing yaml test suites")
	flag.StringVar(&includeTestsRegex, "include-tests", "", "test regex against the file name")

	flag.Parse()
	init_logging()

	if configFile == "" || testSuiteDir == "" {
		log.Fatal().Msg("missing required flags: --config and --test-suite-dir are required")
	}

	if err := loadConfig(configFile); err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	register("truncateDatabase", reflect.ValueOf(fnTruncateDatabase))
	register("recreateKafkaTopic", reflect.ValueOf(fnRecreateKafkaTopic))
	register("producePayload", reflect.ValueOf(fnProducePayload))
	register("bashCmdExecutor", reflect.ValueOf(fnBashCmdExecutor))
	register("sqlExecutor", reflect.ValueOf(fnSqlExecutor))
	register("getKafkaGroupConsumerCurrentOffset", reflect.ValueOf(fnGetKafkaGroupConsumerCurrentOffset))

	log.Info().Msg("initialization completed")
	log.Info().Msgf("kafka brokers: %v, topic: %s, group: %s", cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID)
	log.Info().Msgf("database host: %s port: %d user: %s name: %s sslmode: %s", cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Name, cfg.Database.SSL)
}

// -----------------------------------------------------------------------------
func register(name string, fn reflect.Value) {
	if fn.Kind() != reflect.Func {
		panic("attempt to register non-function")
	}
	registry[name] = fn
	log.Debug().Msgf("registered function: %s", name)
}

// -----------------------------------------------------------------------------
func main() {

	cases, err := loadTestSuites(testSuiteDir)
	if err != nil {
		log.Fatal().Err(err).Msg("failed loading test suites")
	}

	log.Info().Msgf("loaded %d test cases", len(cases))

	for _, tc := range cases {
		if err := runTestCase(tc); err != nil {
			log.Error().Err(err).Msgf("test '%s' failed (file: %s)", tc.Name, tc.FileName)
		}
	}

	log.Info().Msg("all tests completed")
}
