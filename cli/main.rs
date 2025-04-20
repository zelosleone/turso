#![allow(clippy::arc_with_non_send_sync)]
mod app;
mod commands;
mod helper;
mod input;
mod opcodes_dictionary;

#[cfg(unix)]
use nix::unistd::isatty;
use rustyline::{error::ReadlineError, Config, Editor};
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, LazyLock},
};

fn rustyline_config() -> Config {
    Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .auto_add_history(true)
        .build()
}

pub static HOME_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| dirs::home_dir().expect("Could not determine home directory"));

pub static HISTORY_FILE: LazyLock<PathBuf> = LazyLock::new(|| HOME_DIR.join(".limbo_history"));

fn main() -> anyhow::Result<()> {
    let mut app = app::Limbo::new()?;
    let _guard = app.init_tracing()?;

    if is_a_tty() {
        let mut rl = Editor::with_config(rustyline_config())?;
        if HISTORY_FILE.exists() {
            rl.load_history(HISTORY_FILE.as_path())?;
        }
        app = app.with_readline(rl);
    } else {
        tracing::debug!("not in tty");
    }

    loop {
        let readline = app.readline();
        match readline {
            Ok(line) => match app.handle_input_line(line.trim()) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{}", e);
                }
            },
            Err(ReadlineError::Interrupted) => {
                // At prompt, increment interrupt count
                if app.interrupt_count.fetch_add(1, Ordering::SeqCst) >= 1 {
                    eprintln!("Interrupted. Exiting...");
                    let _ = app.close_conn();
                    break;
                }
                println!("Use .quit to exit or press Ctrl-C again to force quit.");
                app.reset_input();
                continue;
            }
            Err(ReadlineError::Eof) => {
                app.handle_remaining_input();
                let _ = app.close_conn();
                break;
            }
            Err(err) => {
                let _ = app.close_conn();
                anyhow::bail!(err)
            }
        }
    }
    Ok(())
}

/// Return whether or not STDIN is a TTY
fn is_a_tty() -> bool {
    #[cfg(unix)]
    {
        isatty(libc::STDIN_FILENO).unwrap_or(false)
    }
    #[cfg(windows)]
    {
        let handle = windows::get_std_handle(windows::console::STD_INPUT_HANDLE);
        match handle {
            Ok(handle) => {
                // If this function doesn't fail then fd is a TTY
                windows::get_console_mode(handle).is_ok()
            }
            Err(_) => false,
        }
    }
}

// Code acquired from Rustyline
#[cfg(windows)]
mod windows {
    use std::io;
    use windows_sys::Win32::Foundation::{self as foundation, BOOL, FALSE, HANDLE};
    pub use windows_sys::Win32::System::Console as console;

    pub fn get_console_mode(handle: HANDLE) -> rustyline::Result<console::CONSOLE_MODE> {
        let mut original_mode = 0;
        check(unsafe { console::GetConsoleMode(handle, &mut original_mode) })?;
        Ok(original_mode)
    }

    pub fn get_std_handle(fd: console::STD_HANDLE) -> rustyline::Result<HANDLE> {
        let handle = unsafe { console::GetStdHandle(fd) };
        check_handle(handle)
    }

    fn check_handle(handle: HANDLE) -> rustyline::Result<HANDLE> {
        if handle == foundation::INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())?;
        } else if handle.is_null() {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "no stdio handle available for this process",
            ))?;
        }
        Ok(handle)
    }

    fn check(rc: BOOL) -> io::Result<()> {
        if rc == FALSE {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
