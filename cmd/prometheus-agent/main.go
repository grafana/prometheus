// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The main package for the Prometheus server executable.
package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/bits"
	"net"
	"net/http"
	_ "net/http/pprof" // Comment this line to disable pprof endpoint.
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/units"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	conntrack "github.com/mwitkow/go-conntrack"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	toolkit_web "github.com/prometheus/exporter-toolkit/web"
	toolkit_webflag "github.com/prometheus/exporter-toolkit/web/kingpinflag"
	jcfg "github.com/uber/jaeger-client-go/config"
	jprom "github.com/uber/jaeger-lib/metrics/prometheus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	klog "k8s.io/klog"
	klogv2 "k8s.io/klog/v2"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	_ "github.com/prometheus/prometheus/discovery/install" // Register service discovery implementations.
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	prom_runtime "github.com/prometheus/prometheus/pkg/runtime"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/storage/wal"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/web"
)

var (
	configSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_config_last_reload_successful",
		Help: "Whether the last configuration reload attempt was successful.",
	})
	configSuccessTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_config_last_reload_success_timestamp_seconds",
		Help: "Timestamp of the last successful configuration reload.",
	})
)

func init() {
	prometheus.MustRegister(version.NewCollector("prometheus_agent"))
}

type flagConfig struct {
	configFile string

	localStoragePath    string
	notifier            notifier.Options
	web                 web.Options
	wal                 walOptions
	lookbackDelta       model.Duration
	webTimeout          model.Duration
	RemoteFlushDeadline model.Duration

	featureList                []string
	enableExpandExternalLabels bool

	prometheusURL   string
	corsRegexString string

	promlogConfig promlog.Config
}

// setFeatureListOptions sets the corresponding options from the featureList.
func (c *flagConfig) setFeatureListOptions(logger log.Logger) error {
	for _, f := range c.featureList {
		opts := strings.Split(f, ",")
		for _, o := range opts {
			switch o {
			case "remote-write-receiver":
				c.web.RemoteWriteReceiver = true
				level.Info(logger).Log("msg", "Experimental remote-write-receiver enabled")
			case "expand-external-labels":
				c.enableExpandExternalLabels = true
				level.Info(logger).Log("msg", "Experimental expand-external-labels enabled")
			case "":
				continue
			default:
				level.Warn(logger).Log("msg", "Unknown option for --enable-feature", "option", o)
			}
		}
	}
	return nil
}

func main() {
	if os.Getenv("DEBUG") != "" {
		runtime.SetBlockProfileRate(20)
		runtime.SetMutexProfileFraction(20)
	}

	cfg := flagConfig{
		notifier: notifier.Options{
			Registerer: prometheus.DefaultRegisterer,
		},
		web: web.Options{
			Registerer: prometheus.DefaultRegisterer,
			Gatherer:   prometheus.DefaultGatherer,
		},
		promlogConfig: promlog.Config{},
	}

	a := kingpin.New(filepath.Base(os.Args[0]), "The Prometheus monitoring server").UsageWriter(os.Stdout)

	a.Version(version.Print("prometheus-agent"))

	a.HelpFlag.Short('h')

	a.Flag("config.file", "Prometheus configuration file path.").
		Default("prometheus.yml").StringVar(&cfg.configFile)

	a.Flag("web.listen-address", "Address to listen on for UI, API, and telemetry.").
		Default("0.0.0.0:9095").StringVar(&cfg.web.ListenAddress)

	webConfig := toolkit_webflag.AddFlags(a)

	a.Flag("web.read-timeout",
		"Maximum duration before timing out read of the request, and closing idle connections.").
		Default("5m").SetValue(&cfg.webTimeout)

	a.Flag("web.max-connections", "Maximum number of simultaneous connections.").
		Default("512").IntVar(&cfg.web.MaxConnections)

	a.Flag("web.external-url",
		"The URL under which Prometheus is externally reachable (for example, if Prometheus is served via a reverse proxy). Used for generating relative and absolute links back to Prometheus itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Prometheus. If omitted, relevant URL components will be derived automatically.").
		PlaceHolder("<URL>").StringVar(&cfg.prometheusURL)

	a.Flag("web.route-prefix",
		"Prefix for the internal routes of web endpoints. Defaults to path of --web.external-url.").
		PlaceHolder("<path>").StringVar(&cfg.web.RoutePrefix)

	a.Flag("web.user-assets", "Path to static asset directory, available at /user.").
		PlaceHolder("<path>").StringVar(&cfg.web.UserAssetsPath)

	a.Flag("web.enable-lifecycle", "Enable shutdown and reload via HTTP request.").
		Default("false").BoolVar(&cfg.web.EnableLifecycle)

	a.Flag("web.enable-admin-api", "Enable API endpoints for admin control actions.").
		Default("false").BoolVar(&cfg.web.EnableAdminAPI)

	a.Flag("web.console.templates", "Path to the console template directory, available at /consoles.").
		Default("consoles").StringVar(&cfg.web.ConsoleTemplatesPath)

	a.Flag("web.console.libraries", "Path to the console library directory.").
		Default("console_libraries").StringVar(&cfg.web.ConsoleLibrariesPath)

	a.Flag("web.page-title", "Document title of Prometheus instance.").
		Default("Prometheus Time Series Collection and Processing Server").StringVar(&cfg.web.PageTitle)

	a.Flag("web.cors.origin", `Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com'`).
		Default(".*").StringVar(&cfg.corsRegexString)

	a.Flag("storage.remote.flush-deadline", "How long to wait flushing sample on shutdown or config reload.").
		Default("1m").PlaceHolder("<duration>").SetValue(&cfg.RemoteFlushDeadline)

	a.Flag("storage.wal.path", "Base path for metrics storage.").
		Default("data-agent/").StringVar(&cfg.localStoragePath)

	a.Flag("storage.wal.segment-size",
		"Size at which to split WAL segment files. Example: 100MB").
		Hidden().PlaceHolder("<bytes>").BytesVar(&cfg.wal.WALSegmentSize)

	a.Flag("storage.wal.compression", "Compress the WAL.").
		Default("true").BoolVar(&cfg.wal.WALCompression)

	a.Flag("storage.wal.truncate-frequency",
		"The frequency at which to truncate the WAL and remove old data.").
		Hidden().PlaceHolder("<duration>").SetValue(&cfg.wal.TruncateFrequency)

	a.Flag("storage.wal.retention.min-time",
		"Minimum age samples may be before being considered for deletion when the WAL is truncated").
		SetValue(&cfg.wal.MinWALTime)

	a.Flag("storage.wal.retention.max-time",
		"Maximum age samples may be before being forcibly deleted when the WAL is truncated").
		SetValue(&cfg.wal.MaxWALTime)

	a.Flag("scrape.adjust-timestamps", "Adjust scrape timestamps by up to 2ms to align them to the intended schedule. See https://github.com/prometheus/prometheus/issues/7846 for more context. Experimental. This flag will be removed in a future release.").
		Hidden().Default("true").BoolVar(&scrape.AlignScrapeTimestamps)

	a.Flag("enable-feature", "Comma separated feature names to enable. Valid options: remote-write-receiver, expand-external-labels. See https://prometheus.io/docs/prometheus/latest/disabled_features/ for more details.").
		Default("").StringsVar(&cfg.featureList)

	promlogflag.AddFlags(a, &cfg.promlogConfig)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	logger := promlog.New(&cfg.promlogConfig)

	if err := cfg.setFeatureListOptions(logger); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing feature list"))
		os.Exit(1)
	}

	cfg.web.ExternalURL, err = computeExternalURL(cfg.prometheusURL, cfg.web.ListenAddress)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "parse external URL %q", cfg.prometheusURL))
		os.Exit(2)
	}

	cfg.web.CORSOrigin, err = compileCORSRegexString(cfg.corsRegexString)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "could not compile CORS regex string %q", cfg.corsRegexString))
		os.Exit(2)
	}

	// Throw error for invalid config before starting other components.
	if _, err := config.LoadFile(cfg.configFile, false, log.NewNopLogger()); err != nil {
		level.Error(logger).Log("msg", fmt.Sprintf("Error loading config (--config.file=%s)", cfg.configFile), "err", err)
		os.Exit(2)
	}
	// Now that the validity of the config is established, set the config
	// success metrics accordingly, although the config isn't really loaded
	// yet. This will happen later (including setting these metrics again),
	// but if we don't do it now, the metrics will stay at zero until the
	// startup procedure is complete, which might take long enough to
	// trigger alerts about an invalid config.
	configSuccess.Set(1)
	configSuccessTime.SetToCurrentTime()

	cfg.web.ReadTimeout = time.Duration(cfg.webTimeout)
	// Default -web.route-prefix to path of -web.external-url.
	if cfg.web.RoutePrefix == "" {
		cfg.web.RoutePrefix = cfg.web.ExternalURL.Path
	}
	// RoutePrefix must always be at least '/'.
	cfg.web.RoutePrefix = "/" + strings.Trim(cfg.web.RoutePrefix, "/")

	// Above level 6, the k8s client would log bearer tokens in clear-text.
	klog.ClampLevel(6)
	klog.SetLogger(log.With(logger, "component", "k8s_client_runtime"))
	klogv2.ClampLevel(6)
	klogv2.SetLogger(log.With(logger, "component", "k8s_client_runtime"))

	level.Info(logger).Log("msg", "Starting Prometheus", "version", version.Info())
	if bits.UintSize < 64 {
		level.Warn(logger).Log("msg", "This Prometheus binary has not been compiled for a 64-bit architecture. Due to virtual memory constraints of 32-bit systems, it is highly recommended to switch to a 64-bit binary of Prometheus.", "GOARCH", runtime.GOARCH)
	}

	level.Info(logger).Log("build_context", version.BuildContext())
	level.Info(logger).Log("host_details", prom_runtime.Uname())
	level.Info(logger).Log("fd_limits", prom_runtime.FdLimits())
	level.Info(logger).Log("vm_limits", prom_runtime.VMLimits())

	var (
		localStorage  = &readyStorage{}
		scraper       = &readyScrapeManager{}
		remoteStorage = remote.NewStorage(log.With(logger, "component", "remote"), prometheus.DefaultRegisterer, localStorage.StartTime, cfg.localStoragePath, time.Duration(cfg.RemoteFlushDeadline), scraper)
		fanoutStorage = storage.NewFanout(logger, localStorage, remoteStorage)
	)

	var (
		ctxWeb, cancelWeb = context.WithCancel(context.Background())

		notifierManager = notifier.NewManager(&cfg.notifier, log.With(logger, "component", "notifier"))

		ctxScrape, cancelScrape = context.WithCancel(context.Background())
		discoveryManagerScrape  = discovery.NewManager(ctxScrape, log.With(logger, "component", "discovery manager scrape"), discovery.Name("scrape"))

		ctxNotify, cancelNotify = context.WithCancel(context.Background())
		discoveryManagerNotify  = discovery.NewManager(ctxNotify, log.With(logger, "component", "discovery manager notify"), discovery.Name("notify"))

		scrapeManager = scrape.NewManager(log.With(logger, "component", "scrape manager"), fanoutStorage)
	)

	scraper.Set(scrapeManager)

	cfg.web.Context = ctxWeb
	cfg.web.TSDBDir = cfg.localStoragePath
	cfg.web.LocalStorage = localStorage
	cfg.web.Storage = fanoutStorage
	cfg.web.ExemplarStorage = localStorage
	cfg.web.ScrapeManager = scrapeManager
	cfg.web.Notifier = notifierManager
	cfg.web.LookbackDelta = time.Duration(cfg.lookbackDelta)
	cfg.web.IsAgent = true

	cfg.web.Version = &web.PrometheusVersion{
		Version:   version.Version,
		Revision:  version.Revision,
		Branch:    version.Branch,
		BuildUser: version.BuildUser,
		BuildDate: version.BuildDate,
		GoVersion: version.GoVersion,
	}

	cfg.web.Flags = map[string]string{}

	// Exclude kingpin default flags to expose only Prometheus ones.
	boilerplateFlags := kingpin.New("", "").Version("")
	for _, f := range a.Model().Flags {
		if boilerplateFlags.GetFlag(f.Name) != nil {
			continue
		}

		cfg.web.Flags[f.Name] = f.Value.String()
	}

	// Depends on cfg.web.ScrapeManager so needs to be after cfg.web.ScrapeManager = scrapeManager.
	webHandler := web.New(log.With(logger, "component", "web"), &cfg.web)

	// Monitor outgoing connections on default transport with conntrack.
	http.DefaultTransport.(*http.Transport).DialContext = conntrack.NewDialContextFunc(
		conntrack.DialWithTracing(),
	)

	reloaders := []reloader{
		{
			name:     "remote_storage",
			reloader: remoteStorage.ApplyConfig,
		}, {
			name:     "web_handler",
			reloader: webHandler.ApplyConfig,
		}, {
			// The Scrape and notifier managers need to reload before the Discovery manager as
			// they need to read the most updated config when receiving the new targets list.
			name:     "scrape",
			reloader: scrapeManager.ApplyConfig,
		}, {
			name: "scrape_sd",
			reloader: func(cfg *config.Config) error {
				c := make(map[string]discovery.Configs)
				for _, v := range cfg.ScrapeConfigs {
					c[v.JobName] = v.ServiceDiscoveryConfigs
				}
				return discoveryManagerScrape.ApplyConfig(c)
			},
		}, {
			name:     "notify",
			reloader: notifierManager.ApplyConfig,
		}, {
			name: "notify_sd",
			reloader: func(cfg *config.Config) error {
				c := make(map[string]discovery.Configs)
				for k, v := range cfg.AlertingConfig.AlertmanagerConfigs.ToMap() {
					c[k] = v.ServiceDiscoveryConfigs
				}
				return discoveryManagerNotify.ApplyConfig(c)
			},
		},
	}

	prometheus.MustRegister(configSuccess, configSuccessTime)

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.
	dbOpen := make(chan struct{})

	// sync.Once is used to make sure we can close the channel at different execution stages(SIGTERM or when the config is loaded).
	type closeOnce struct {
		C     chan struct{}
		once  sync.Once
		Close func()
	}
	// Wait until the server is ready to handle reloading.
	reloadReady := &closeOnce{
		C: make(chan struct{}),
	}
	reloadReady.Close = func() {
		reloadReady.once.Do(func() {
			close(reloadReady.C)
		})
	}

	closer, err := initTracing(logger)
	if err != nil {
		level.Error(logger).Log("msg", "Unable to init tracing", "err", err)
		os.Exit(2)
	}
	defer closer.Close()

	listener, err := webHandler.Listener()
	if err != nil {
		level.Error(logger).Log("msg", "Unable to start web listener", "err", err)
		os.Exit(1)
	}

	err = toolkit_web.Validate(*webConfig)
	if err != nil {
		level.Error(logger).Log("msg", "Unable to validate web configuration file", "err", err)
		os.Exit(1)
	}

	var g run.Group
	{
		// Termination handler.
		term := make(chan os.Signal, 1)
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)
		cancel := make(chan struct{})
		g.Add(
			func() error {
				// Don't forget to release the reloadReady channel so that waiting blocks can exit normally.
				select {
				case <-term:
					level.Warn(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
					reloadReady.Close()
				case <-webHandler.Quit():
					level.Warn(logger).Log("msg", "Received termination request via web service, exiting gracefully...")
				case <-cancel:
					reloadReady.Close()
				}
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	{
		// Scrape discovery manager.
		g.Add(
			func() error {
				err := discoveryManagerScrape.Run()
				level.Info(logger).Log("msg", "Scrape discovery manager stopped")
				return err
			},
			func(err error) {
				level.Info(logger).Log("msg", "Stopping scrape discovery manager...")
				cancelScrape()
			},
		)
	}
	{
		// Notify discovery manager.
		g.Add(
			func() error {
				err := discoveryManagerNotify.Run()
				level.Info(logger).Log("msg", "Notify discovery manager stopped")
				return err
			},
			func(err error) {
				level.Info(logger).Log("msg", "Stopping notify discovery manager...")
				cancelNotify()
			},
		)
	}
	{
		// Scrape manager.
		g.Add(
			func() error {
				// When the scrape manager receives a new targets list
				// it needs to read a valid config for each job.
				// It depends on the config being in sync with the discovery manager so
				// we wait until the config is fully loaded.
				<-reloadReady.C

				err := scrapeManager.Run(discoveryManagerScrape.SyncCh())
				level.Info(logger).Log("msg", "Scrape manager stopped")
				return err
			},
			func(err error) {
				// Scrape manager needs to be stopped before closing the local TSDB
				// so that it doesn't try to write samples to a closed storage.
				level.Info(logger).Log("msg", "Stopping scrape manager...")
				scrapeManager.Stop()
			},
		)
	}
	{
		// Reload handler.

		// Make sure that sighup handler is registered with a redirect to the channel before the potentially
		// long and synchronous tsdb init.
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		cancel := make(chan struct{})
		g.Add(
			func() error {
				<-reloadReady.C

				for {
					select {
					case <-hup:
						if err := reloadConfig(cfg.configFile, cfg.enableExpandExternalLabels, logger, reloaders...); err != nil {
							level.Error(logger).Log("msg", "Error reloading config", "err", err)
						}
					case rc := <-webHandler.Reload():
						if err := reloadConfig(cfg.configFile, cfg.enableExpandExternalLabels, logger, reloaders...); err != nil {
							level.Error(logger).Log("msg", "Error reloading config", "err", err)
							rc <- err
						} else {
							rc <- nil
						}
					case <-cancel:
						return nil
					}
				}

			},
			func(err error) {
				// Wait for any in-progress reloads to complete to avoid
				// reloading things after they have been shutdown.
				cancel <- struct{}{}
			},
		)
	}
	{
		// Initial configuration loading.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				select {
				case <-dbOpen:
				// In case a shutdown is initiated before the dbOpen is released
				case <-cancel:
					reloadReady.Close()
					return nil
				}

				if err := reloadConfig(cfg.configFile, cfg.enableExpandExternalLabels, logger, reloaders...); err != nil {
					return errors.Wrapf(err, "error loading config from %q", cfg.configFile)
				}

				reloadReady.Close()

				webHandler.Ready()
				level.Info(logger).Log("msg", "Server is ready to receive web requests.")
				<-cancel
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	{
		// WAL storage.
		opts := cfg.wal.ToWALOptions()
		cancel := make(chan struct{})
		g.Add(
			func() error {
				level.Info(logger).Log("msg", "Starting WAL storage ...")
				if cfg.wal.WALSegmentSize != 0 {
					if cfg.wal.WALSegmentSize < 10*1024*1024 || cfg.wal.WALSegmentSize > 256*1024*1024 {
						return errors.New("flag 'storage.wal.segment-size' must be set between 10MB and 256MB")
					}
				}
				db, err := wal.NewStorage(
					logger,
					prometheus.DefaultRegisterer,
					remoteStorage,
					cfg.localStoragePath,
					&opts,
				)
				if err != nil {
					return errors.Wrap(err, "opening storage failed")
				}

				switch fsType := prom_runtime.Statfs(cfg.localStoragePath); fsType {
				case "NFS_SUPER_MAGIC":
					level.Warn(logger).Log("fs_type", fsType, "msg", "This filesystem is not supported and may lead to data corruption and data loss. Please carefully read https://prometheus.io/docs/prometheus/latest/storage/ to learn more about supported filesystems.")
				default:
					level.Info(logger).Log("fs_type", fsType)
				}

				level.Info(logger).Log("msg", "WAL storage started")
				level.Debug(logger).Log("msg", "WAL storage options",
					"WALSegmentSize", cfg.wal.WALSegmentSize,
					"WALCompression", cfg.wal.WALCompression,
					"StripeSize", cfg.wal.StripeSize,
					"TruncateFrequency", cfg.wal.TruncateFrequency,
					"MinWALTime", cfg.wal.MinWALTime,
					"MaxWALTime", cfg.wal.MaxWALTime,
				)

				localStorage.Set(db)
				close(dbOpen)
				<-cancel
				return nil
			},
			func(e error) {
				if err := fanoutStorage.Close(); err != nil {
					level.Error(logger).Log("msg", "Error stopping storage", "err", err)
				}
				close(cancel)
			},
		)

	}
	{
		// Web handler.
		g.Add(
			func() error {
				if err := webHandler.Run(ctxWeb, listener, *webConfig); err != nil {
					return errors.Wrapf(err, "error starting web server")
				}
				return nil
			},
			func(err error) {
				cancelWeb()
			},
		)
	}
	{
		// Notifier.

		// Calling notifier.Stop() before ruleManager.Stop() will cause a panic if the ruleManager isn't running,
		// so keep this interrupt after the ruleManager.Stop().
		g.Add(
			func() error {
				// When the notifier manager receives a new targets list
				// it needs to read a valid config for each job.
				// It depends on the config being in sync with the discovery manager
				// so we wait until the config is fully loaded.
				<-reloadReady.C

				notifierManager.Run(discoveryManagerNotify.SyncCh())
				level.Info(logger).Log("msg", "Notifier manager stopped")
				return nil
			},
			func(err error) {
				notifierManager.Stop()
			},
		)
	}
	if err := g.Run(); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "See you next time!")
}

type reloader struct {
	name     string
	reloader func(*config.Config) error
}

func reloadConfig(filename string, expandExternalLabels bool, logger log.Logger, rls ...reloader) (err error) {
	start := time.Now()
	timings := []interface{}{}
	level.Info(logger).Log("msg", "Loading configuration file", "filename", filename)

	defer func() {
		if err == nil {
			configSuccess.Set(1)
			configSuccessTime.SetToCurrentTime()
		} else {
			configSuccess.Set(0)
		}
	}()

	conf, err := config.LoadFile(filename, expandExternalLabels, logger)
	if err != nil {
		return errors.Wrapf(err, "couldn't load configuration (--config.file=%q)", filename)
	}

	// Perform validation for Agent-compatible configs and remove anything that's
	// unsupported.
	if len(conf.AlertingConfig.AlertRelabelConfigs) > 0 || len(conf.AlertingConfig.AlertmanagerConfigs) > 0 {
		level.Warn(logger).Log("msg", "alerting configs not supported in prometheus-agent")
		conf.AlertingConfig.AlertRelabelConfigs = []*relabel.Config{}
		conf.AlertingConfig.AlertmanagerConfigs = config.AlertmanagerConfigs{}
	}
	if len(conf.RuleFiles) > 0 {
		level.Warn(logger).Log("msg", "recording rules not supported in prometheus-agent")
		conf.RuleFiles = []string{}
	}
	if len(conf.RemoteReadConfigs) > 0 {
		level.Warn(logger).Log("msg", "remote_read configs not supported in prometheus-agent")
		conf.RemoteReadConfigs = []*config.RemoteReadConfig{}
	}

	failed := false
	for _, rl := range rls {
		rstart := time.Now()
		if err := rl.reloader(conf); err != nil {
			level.Error(logger).Log("msg", "Failed to apply configuration", "err", err)
			failed = true
		}
		timings = append(timings, rl.name, time.Since(rstart))
	}
	if failed {
		return errors.Errorf("one or more errors occurred while applying the new configuration (--config.file=%q)", filename)
	}

	l := []interface{}{"msg", "Completed loading of configuration file", "filename", filename, "totalDuration", time.Since(start)}
	level.Info(logger).Log(append(l, timings...)...)
	return nil
}

func startsOrEndsWithQuote(s string) bool {
	return strings.HasPrefix(s, "\"") || strings.HasPrefix(s, "'") ||
		strings.HasSuffix(s, "\"") || strings.HasSuffix(s, "'")
}

// compileCORSRegexString compiles given string and adds anchors
func compileCORSRegexString(s string) (*regexp.Regexp, error) {
	r, err := relabel.NewRegexp(s)
	if err != nil {
		return nil, err
	}
	return r.Regexp, nil
}

// computeExternalURL computes a sanitized external URL from a raw input. It infers unset
// URL parts from the OS and the given listen address.
func computeExternalURL(u, listenAddr string) (*url.URL, error) {
	if u == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		_, port, err := net.SplitHostPort(listenAddr)
		if err != nil {
			return nil, err
		}
		u = fmt.Sprintf("http://%s:%s/", hostname, port)
	}

	if startsOrEndsWithQuote(u) {
		return nil, errors.New("URL must not begin or end with quotes")
	}

	eu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	ppref := strings.TrimRight(eu.Path, "/")
	if ppref != "" && !strings.HasPrefix(ppref, "/") {
		ppref = "/" + ppref
	}
	eu.Path = ppref

	return eu, nil
}

// readyStorage implements the Storage interface while allowing to set the actual
// storage at a later point in time.
type readyStorage struct {
	mtx sync.RWMutex
	db  *wal.Storage
}

// Set the storage.
func (s *readyStorage) Set(db *wal.Storage) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.db = db
}

// get is internal, you should use readyStorage as the front implementation layer.
func (s *readyStorage) get() *wal.Storage {
	s.mtx.RLock()
	x := s.db
	s.mtx.RUnlock()
	return x
}

// StartTime implements the Storage interface.
func (s *readyStorage) StartTime() (int64, error) {
	if x := s.get(); x != nil {
		return x.StartTime()
	}
	return math.MaxInt64, tsdb.ErrNotReady
}

// Querier implements the Storage interface.
func (s *readyStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if x := s.get(); x != nil {
		return x.Querier(ctx, mint, maxt)
	}
	return nil, tsdb.ErrNotReady
}

// ChunkQuerier implements the Storage interface.
func (s *readyStorage) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	if x := s.get(); x != nil {
		return x.ChunkQuerier(ctx, mint, maxt)
	}
	return nil, tsdb.ErrNotReady
}

func (s *readyStorage) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	if x := s.get(); x != nil {
		return x.ExemplarQuerier(ctx)
	}
	return nil, tsdb.ErrNotReady
}

// Appender implements the Storage interface.
func (s *readyStorage) Appender(ctx context.Context) storage.Appender {
	if x := s.get(); x != nil {
		return x.Appender(ctx)
	}
	return notReadyAppender{}
}

type notReadyAppender struct{}

func (n notReadyAppender) Append(ref uint64, l labels.Labels, t int64, v float64) (uint64, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) AppendExemplar(ref uint64, l labels.Labels, e exemplar.Exemplar) (uint64, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) Commit() error { return tsdb.ErrNotReady }

func (n notReadyAppender) Rollback() error { return tsdb.ErrNotReady }

// Close implements the Storage interface.
func (s *readyStorage) Close() error {
	if x := s.get(); x != nil {
		return x.Close()
	}
	return nil
}

// CleanTombstones implements the api_v1.TSDBAdminStats and api_v2.TSDBAdmin interfaces.
func (s *readyStorage) CleanTombstones() error {
	return wal.ErrUnsupported
}

// Delete implements the api_v1.TSDBAdminStats and api_v2.TSDBAdmin interfaces.
func (s *readyStorage) Delete(mint, maxt int64, ms ...*labels.Matcher) error {
	return wal.ErrUnsupported
}

// Snapshot implements the api_v1.TSDBAdminStats and api_v2.TSDBAdmin interfaces.
func (s *readyStorage) Snapshot(dir string, withHead bool) error {
	return wal.ErrUnsupported
}

// Stats implements the api_v1.TSDBAdminStats interface.
func (s *readyStorage) Stats(statsByLabelName string) (*tsdb.Stats, error) {
	return nil, wal.ErrUnsupported
}

// ErrNotReady is returned if the underlying scrape manager is not ready yet.
var ErrNotReady = errors.New("Scrape manager not ready")

// ReadyScrapeManager allows a scrape manager to be retrieved. Even if it's set at a later point in time.
type readyScrapeManager struct {
	mtx sync.RWMutex
	m   *scrape.Manager
}

// Set the scrape manager.
func (rm *readyScrapeManager) Set(m *scrape.Manager) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	rm.m = m
}

// Get the scrape manager. If is not ready, return an error.
func (rm *readyScrapeManager) Get() (*scrape.Manager, error) {
	rm.mtx.RLock()
	defer rm.mtx.RUnlock()

	if rm.m != nil {
		return rm.m, nil
	}

	return nil, ErrNotReady
}

func initTracing(logger log.Logger) (io.Closer, error) {
	// Set tracing configuration defaults.
	cfg := &jcfg.Configuration{
		ServiceName: "prometheus",
		Disabled:    true,
	}

	// Available options can be seen here:
	// https://github.com/jaegertracing/jaeger-client-go#environment-variables
	cfg, err := cfg.FromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get tracing config from environment")
	}

	jLogger := jaegerLogger{logger: log.With(logger, "component", "tracing")}

	tracer, closer, err := cfg.NewTracer(
		jcfg.Logger(jLogger),
		jcfg.Metrics(jprom.New()),
	)
	if err != nil {
		return nil, errors.Wrap(err, "unable to init tracing")
	}

	opentracing.SetGlobalTracer(tracer)
	return closer, nil
}

// walOptions is a version of wal.Options with defined units. This is required
// as wal.Option fields are unit agnostic (time).
type walOptions struct {
	WALSegmentSize         units.Base2Bytes
	WALCompression         bool
	StripeSize             int
	TruncateFrequency      model.Duration
	MinWALTime, MaxWALTime model.Duration
}

func (opts walOptions) ToWALOptions() wal.Options {
	return wal.Options{
		WALSegmentSize:    int(opts.WALSegmentSize),
		WALCompression:    opts.WALCompression,
		StripeSize:        opts.StripeSize,
		TruncateFrequency: time.Duration(opts.TruncateFrequency),
		MinWALTime:        time.Duration(opts.MinWALTime),
		MaxWALTime:        time.Duration(opts.MaxWALTime),
	}
}

type jaegerLogger struct {
	logger log.Logger
}

func (l jaegerLogger) Error(msg string) {
	level.Error(l.logger).Log("msg", msg)
}

func (l jaegerLogger) Infof(msg string, args ...interface{}) {
	keyvals := []interface{}{"msg", fmt.Sprintf(msg, args...)}
	level.Info(l.logger).Log(keyvals...)
}
