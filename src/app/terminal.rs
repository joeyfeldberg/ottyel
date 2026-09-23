//! Terminal mode ownership: every exit path, including errors and panics, restores the shell.

use std::{
    io::{self, Write},
    sync::Once,
};

use anyhow::Result;
use crossterm::{
    cursor::Show,
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};

static PANIC_HOOK: Once = Once::new();

/// Holds raw mode, the alternate screen, and mouse capture until dropped.
pub(super) struct TerminalGuard {
    _private: (),
}

impl TerminalGuard {
    pub(super) fn enter() -> Result<Self> {
        install_panic_hook();
        enable_raw_mode()?;
        // Constructed before the escape sequences so a failure between them still restores.
        let guard = Self { _private: () };
        execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

/// Restores the terminal before the default hook prints, so the panic message stays visible
/// on the normal screen instead of vanishing with the alternate screen.
fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            restore_terminal();
            previous(info);
        }));
    });
}

fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = write_restore_sequence(&mut io::stdout());
}

/// Leaving modes that are not active is harmless, so restoring twice is safe.
fn write_restore_sequence(out: &mut impl Write) -> io::Result<()> {
    execute!(out, DisableMouseCapture, LeaveAlternateScreen, Show)
}

#[cfg(test)]
mod tests {
    #[test]
    fn restore_sequence_leaves_the_alternate_screen_and_shows_the_cursor() {
        let mut out = Vec::new();
        super::write_restore_sequence(&mut out).unwrap();
        let text = String::from_utf8(out).unwrap();

        assert!(text.contains("\u{1b}[?1049l"), "{text:?}");
        assert!(text.contains("\u{1b}[?25h"), "{text:?}");
        assert!(text.contains("\u{1b}[?1000l"), "{text:?}");
    }
}
