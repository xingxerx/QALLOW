#![allow(dead_code)]

mod backend;
mod button_handlers;
mod clipboard;
mod codebase_manager;
mod config;
mod control_commands;
mod dungeons;
mod error_recovery;
mod gpu;
mod logging;
mod messaging;
mod models;
mod shortcuts;
mod shutdown;
mod telemetry;
mod ui;
mod utils;

use backend::process_manager::ProcessManager;
use button_handlers::ButtonHandler;
use codebase_manager::CodebaseManager;
use config::{AppConfig, ConfigManager};
use fltk::enums::Color;
use fltk::{app, button, prelude::*};
use models::AppState;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use crate::{
    logging::AppLogger,
    messaging::UiMessage,
    shutdown::ShutdownManager,
};
#[cfg(not(any(target_os = "windows", target_os = "macos")))]
use std::env;
use std::io;
#[cfg(not(any(target_os = "windows", target_os = "macos")))]
use std::path::Path;
use std::process::{Command, Stdio};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

enum VmStatus {
    Running,
    Paused,
    Stopped,
}

fn main() {
    let rt = Runtime::new().unwrap();
    let _enter = rt.enter();
    env_logger::init();

    // Initialize configuration
    let config_mgr = ConfigManager::new("qallow_config.json".to_string());
    let config = config_mgr.get().clone();

    // Initialize logger
    let logger = AppLogger::new(
        config.logging.file_path.clone(),
        config.logging.max_file_size_mb,
        config.logging.max_backups,
    );
    let _ = logger.init();
    let _ = logger.info("🚀 Qallow Application Starting");

    // Initialize GPU acceleration
    let gpu_capability = gpu::check_gpu_availability();
    let _ = logger.info(&format!("GPU Capability: {}", gpu_capability));

    let _gpu_manager = match gpu::GPUManager::new() {
        Ok(mgr) => {
            let metrics = mgr.get_metrics();
            let _ = logger.info(&format!(
                "✓ GPU Initialized: {} (Compute {}.{})",
                metrics.device_name, metrics.compute_capability.0, metrics.compute_capability.1
            ));
            Some(Arc::new(mgr))
        }
        Err(e) => {
            let _ = logger.warn(&format!("GPU initialization failed: {}", e));
            None
        }
    };

    // Initialize codebase manager
    let codebase_mgr = match CodebaseManager::new("/root/Qallow", logger.clone()) {
        Ok(mgr) => {
            let _ = logger.info("✓ Codebase manager initialized");
            Some(Arc::new(mgr))
        }
        Err(e) => {
            let _ = logger.warn(&format!("Could not initialize codebase manager: {}", e));
            None
        }
    };

    // Initialize shutdown manager
    let shutdown_mgr = ShutdownManager::new("qallow_state.json".to_string());
    ShutdownManager::init_signal_handlers();

    // Load previous state if available
    let initial_state = match shutdown_mgr.load_state() {
        Ok(state) => {
            let _ = logger.info("✓ Previous state loaded successfully");
            state
        }
        Err(e) => {
            let _ = logger.warn(&format!("Could not load previous state: {}", e));
            models::AppState::new()
        }
    };

    // Initialize FLTK
    let app = app::App::default();
    // UI message channel for background tasks
    let (sender, receiver) = app::channel::<UiMessage>();

    // Don't apply theme - let individual widget colors show through
    // The theme was overriding our modern neon colors
    // let theme = fltk_theme::WidgetTheme::new(ThemeType::Dark);
    // theme.apply();

    // Create application state
    let state = Arc::new(Mutex::new(initial_state));

    // Create process manager
    let process_manager = Arc::new(Mutex::new(ProcessManager::new()));

    // Create button handler
    let button_handler = Arc::new(ButtonHandler::new(
        state.clone(),
        process_manager.clone(),
        Arc::new(logger.clone()),
        codebase_mgr.clone(),
        Some(sender),
    ));

    // --- Main Window and UI Setup ---
    let mut main_win = ui::main_window::MainWindow::new(button_handler.clone());

    for i in 0..4 {
        let phase = models::Phase::from_index(i).unwrap();
        main_win.control_panel.buttons.phase_buttons[i].set_callback({
            let handler = main_win.button_handler.clone();
            move |_| {
                if let Err(e) = handler.on_run_phase(phase) {
                    println!("Error running phase: {}", e);
                }
            }
        });
    }

    main_win.control_panel.buttons.unified_button.set_callback({
        let handler = main_win.button_handler.clone();
        move |_| {
            if let Err(e) = handler.on_run_phase(models::Phase::Unified) {
                println!("Error running unified: {}", e);
            }
        }
    });

    main_win.wind.show();

    // --- Main Application Loop ---
    let logger_clone = logger.clone();

    // Use the standard FLTK event loop.
    // `app.wait()` will block here until the window is closed.
    while app.wait() {
        // Handle messages from background threads
        if let Some(_msg) = receiver.recv() {
            {}
        }

        // Update UI elements that need periodic refresh
        if let Ok(mut state_guard) = state.lock() {
            state_guard.update_uptime();
        }

        // Check for global shutdown signal
        if shutdown::SHUTDOWN_FLAG.load(std::sync::atomic::Ordering::SeqCst) {
            let _ = logger_clone.info("⚠ Shutdown signal received, saving state...");
            if let Ok(state_guard) = state.lock() {
                 if let Err(e) = shutdown_mgr.save_state(&state_guard) {
                    let _ = logger_clone.error(&format!("Failed to save state: {}", e));
                }
            }
            let _ = shutdown_mgr.cleanup();
            app.quit();
        }
    }

    let _ = logger.info("✓ Application exiting gracefully");

    // Explicitly drop the runtime to wait for background tasks.
    drop(rt);
}

// This function is kept as a placeholder for future CLI implementation.
fn run_cli_interface(
    _app_state: Arc<Mutex<AppState>>,
    _logger: AppLogger,
    _shutdown_mgr: ShutdownManager,
) -> Result<(), io::Error> {
    println!("CLI mode is not yet fully implemented.");
    Ok(())
}

/// Updates the color and label of the VM status button.
fn update_vm_status_indicator(button: &mut button::Button, status: VmStatus) {
    match status {
        VmStatus::Running => {
            button.set_label("● Running");
            button.set_color(Color::from_hex(0x00ff64));
            button.set_label_color(Color::Black);
        }
        VmStatus::Paused => {
            button.set_label("● Paused");
            button.set_color(Color::from_hex(0xffaa00));
            button.set_label_color(Color::Black);
        }
        VmStatus::Stopped => {
            button.set_label("● Stopped");
            button.set_color(Color::from_hex(0xff6464));
            button.set_label_color(Color::White);
        }
    }
    button.redraw();
}

fn display_available() -> bool {
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    {
        true
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        if let Ok(display) = env::var("DISPLAY") {
            if display.starts_with(':') {
                let socket = display
                    .trim_start_matches(':')
                    .split('.')
                    .next()
                    .unwrap_or("0");
                let socket_path = format!("/tmp/.X11-unix/X{}", socket);
                if Path::new(&socket_path).exists() {
                    return UnixStream::connect(socket_path).is_ok();
                }
            } else if display.starts_with("unix:") {
                let socket = display
                    .trim_start_matches("unix:")
                    .split('.')
                    .next()
                    .unwrap_or("0");
                let socket_path = format!("/tmp/.X11-unix/X{}", socket);
                if Path::new(&socket_path).exists() {
                    return UnixStream::connect(socket_path).is_ok();
                }
            }
        }
        if env::var("WAYLAND_DISPLAY").is_ok() {
            return true;
        }
        false
    }
}

fn run_headless(config: &AppConfig, logger: &AppLogger) -> Result<(), String> {
    let mut command = Command::new("./build/qallow");
    command.arg("run");

    let phase_lower = config.vm.default_phase.to_lowercase();
    if phase_lower == "phase2" || phase_lower == "2" {
        command.arg("--phase=2");
        command.arg(format!("--ticks={}", config.vm.default_ticks));
    } else if phase_lower == "phase4" || phase_lower == "4" {
        command.arg("--phase=4");
        command.arg(format!("--ticks={}", config.vm.default_ticks));
    } else if phase_lower == "phase3" || phase_lower == "3" || phase_lower == "unified" {
        command.arg("unified");
    } else {
        command.arg("unified");
        let _ = logger.warn(&format!(
            "Unknown default phase '{}'; defaulting to unified pipeline",
            config.vm.default_phase
        ));
    }

    if config.vm.default_build.eq_ignore_ascii_case("cuda") {
        command.env("QALLOW_PREFERRED_BUILD", "CUDA");
    }

    command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    let debug_cmd = format!("{:?}", command);
    let _ = logger.info(&format!("▶ Running headless pipeline via {}", debug_cmd));

    let status = command
        .status()
        .map_err(|e| format!("Failed to launch CLI run: {}", e))?;

    if !status.success() {
        return Err(format!("CLI run exited with status: {}", status));
    }

    Ok(())
}
