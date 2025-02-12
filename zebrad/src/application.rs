//! Zebrad Abscissa Application

use crate::{commands::ZebradCmd, config::ZebradConfig};
use abscissa_core::{
    application::{self, AppCell},
    config,
    terminal::component::Terminal,
    trace::Tracing,
    Application, Component, EntryPoint, FrameworkError, StandardPaths,
};

/// Application state
pub static APPLICATION: AppCell<ZebradApp> = AppCell::new();

/// Obtain a read-only (multi-reader) lock on the application state.
///
/// Panics if the application state has not been initialized.
pub fn app_reader() -> application::lock::Reader<ZebradApp> {
    APPLICATION.read()
}

/// Obtain an exclusive mutable lock on the application state.
pub fn app_writer() -> application::lock::Writer<ZebradApp> {
    APPLICATION.write()
}

/// Obtain a read-only (multi-reader) lock on the application configuration.
///
/// Panics if the application configuration has not been loaded.
pub fn app_config() -> config::Reader<ZebradApp> {
    config::Reader::new(&APPLICATION)
}

/// Zebrad Application
#[derive(Debug)]
pub struct ZebradApp {
    /// Application configuration.
    config: Option<ZebradConfig>,

    /// Application state.
    state: application::State<Self>,
}

/// Initialize a new application instance.
///
/// By default no configuration is loaded, and the framework state is
/// initialized to a default, empty state (no components, threads, etc).
impl Default for ZebradApp {
    fn default() -> Self {
        Self {
            config: None,
            state: application::State::default(),
        }
    }
}

impl Application for ZebradApp {
    /// Entrypoint command for this application.
    type Cmd = EntryPoint<ZebradCmd>;

    /// Application configuration.
    type Cfg = ZebradConfig;

    /// Paths to resources within the application.
    type Paths = StandardPaths;

    /// Accessor for application configuration.
    fn config(&self) -> &ZebradConfig {
        self.config.as_ref().expect("config not loaded")
    }

    /// Borrow the application state immutably.
    fn state(&self) -> &application::State<Self> {
        &self.state
    }

    /// Borrow the application state mutably.
    fn state_mut(&mut self) -> &mut application::State<Self> {
        &mut self.state
    }

    /// Returns the framework components used by this application.
    fn framework_components(
        &mut self,
        command: &Self::Cmd,
    ) -> Result<Vec<Box<dyn Component<Self>>>, FrameworkError> {
        color_eyre::install().unwrap();

        let terminal = Terminal::new(self.term_colors(command));
        if ZebradApp::command_is_server(&command) {
            let tracing = self.tracing_component(command);
            Ok(vec![Box::new(terminal), Box::new(tracing)])
        } else {
            Ok(vec![Box::new(terminal)])
        }
    }

    /// Register all components used by this application.
    ///
    /// If you would like to add additional components to your application
    /// beyond the default ones provided by the framework, this is the place
    /// to do so.
    fn register_components(&mut self, command: &Self::Cmd) -> Result<(), FrameworkError> {
        use crate::components::{
            metrics::MetricsEndpoint, tokio::TokioComponent, tracing::TracingEndpoint,
        };

        let mut components = self.framework_components(command)?;
        // Launch network endpoints for long-running commands
        if ZebradApp::command_is_server(&command) {
            components.push(Box::new(TokioComponent::new()?));
            components.push(Box::new(TracingEndpoint::new()?));
            components.push(Box::new(MetricsEndpoint::new()?));
        }

        self.state.components.register(components)
    }

    /// Post-configuration lifecycle callback.
    ///
    /// Called regardless of whether config is loaded to indicate this is the
    /// time in app lifecycle when configuration would be loaded if
    /// possible.
    fn after_config(
        &mut self,
        config: Self::Cfg,
        command: &Self::Cmd,
    ) -> Result<(), FrameworkError> {
        use crate::components::{
            metrics::MetricsEndpoint, tokio::TokioComponent, tracing::TracingEndpoint,
        };

        // Configure components
        self.state.components.after_config(&config)?;
        self.config = Some(config);

        if ZebradApp::command_is_server(&command) {
            let level = self.level(command);
            self.state
                .components
                .get_downcast_mut::<Tracing>()
                .expect("Tracing component should be available")
                .reload_filter(level);

            // Work around some issues with dependency injection and configs
            let config = self
                .config
                .clone()
                .expect("config was set to Some earlier in this function");

            let tokio_component = self
                .state
                .components
                .get_downcast_ref::<TokioComponent>()
                .expect("Tokio component should be available");

            self.state
                .components
                .get_downcast_ref::<TracingEndpoint>()
                .expect("Tracing endpoint should be available")
                .open_endpoint(&config.tracing, tokio_component);

            self.state
                .components
                .get_downcast_ref::<MetricsEndpoint>()
                .expect("Metrics endpoint should be available")
                .open_endpoint(&config.metrics, tokio_component);
        }

        Ok(())
    }
}

impl ZebradApp {
    fn level(&self, command: &EntryPoint<ZebradCmd>) -> String {
        // `None` outputs zebrad usage information to stdout
        let command_uses_stdout = match &command.command {
            None => true,
            Some(c) => c.uses_stdout(),
        };

        // Allow users to:
        //  - override all other configs and defaults using the command line
        //  - see command outputs without spurious log messages, by default
        //  - override the config file using an environmental variable
        if command.verbose {
            "debug".to_string()
        } else if command_uses_stdout {
            // Tracing sends output to stdout, so we disable info-level logs for
            // some commands.
            //
            // TODO: send tracing output to stderr. This change requires an abscissa
            //       update, because `abscissa_core::component::Tracing` uses
            //       `tracing_subscriber::fmt::Formatter`, which has `Stdout` as a
            //       type parameter. We need `MakeWriter` or a similar type.
            "warn".to_string()
        } else if let Ok(level) = std::env::var("ZEBRAD_LOG") {
            level
        } else if let Some(ZebradConfig {
            tracing:
                crate::config::TracingSection {
                    filter: Some(filter),
                    endpoint_addr: _,
                },
            ..
        }) = &self.config
        {
            filter.clone()
        } else {
            "info".to_string()
        }
    }

    fn tracing_component(&self, command: &EntryPoint<ZebradCmd>) -> Tracing {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

        // Construct a tracing subscriber with the supplied filter and enable reloading.
        let builder = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(self.level(command))
            .with_filter_reloading();
        let filter_handle = builder.reload_handle();

        builder
            .finish()
            .with(tracing_error::ErrorLayer::default())
            .init();

        filter_handle.into()
    }

    /// Returns true if command is a server command.
    ///
    /// Server commands use long-running components such as tracing, metrics,
    /// and the tokio runtime.
    fn command_is_server(command: &EntryPoint<ZebradCmd>) -> bool {
        // `None` outputs zebrad usage information and exits
        match &command.command {
            None => false,
            Some(c) => c.is_server(),
        }
    }
}
