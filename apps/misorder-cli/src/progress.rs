//! A progress bar for a sweep.
//!
//! Ten thousand seeds is minutes, and a tool that prints nothing for minutes is
//! one people `Ctrl-C` to check it is alive. What this answers is the two
//! things someone actually wants during that wait: how far along it is, and
//! whether it is finding anything.
//!
//! # Why it is hand-rolled
//!
//! Carriage return, erase-line, and a bar made of two characters. A progress
//! bar crate would be a line in a dependency list that people in this segment
//! read as part of a security review, and it would buy spinners.
//!
//! # When it draws
//!
//! Only when stderr is a terminal. Redirected output gets nothing at all -
//! not a plain-text version, nothing - because a progress bar in a log file is
//! ten thousand lines of noise around the one line that mattered.
//!
//! Colour is separate: the bar draws whether or not colour is on, and uses the
//! palette it is given. Someone with `NO_COLOR` set still wants to know how far
//! along their sweep is.

use std::io::{IsTerminal, Write};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use misorder::report::Style;
use misorder::runner::Progress;

/// Redraws no more often than this.
///
/// A sweep of loopback runs completes hundreds per second, and redrawing on
/// every one spends more time formatting than running. Twenty a second is
/// smooth to a person and free to a machine.
const FRAME: Duration = Duration::from_millis(50);

/// How wide the bar itself is, in characters.
const WIDTH: usize = 28;

/// Draws a sweep's progress, or does nothing at all.
pub struct Bar {
    style: Style,
    /// `None` when nothing should be drawn, which is the whole of the
    /// not-a-terminal case: there is no plain fallback to get wrong.
    state: Option<Mutex<State>>,
}

struct State {
    last_drawn: Instant,
    /// Whether anything is currently on the line, so that finishing knows
    /// whether it has something to erase.
    dirty: bool,
}

impl Bar {
    /// A bar for this process, drawing only if stderr is a terminal.
    pub fn new(style: Style) -> Self {
        let state = std::io::stderr().is_terminal().then(|| {
            Mutex::new(State {
                // Far enough in the past that the first update draws rather
                // than being rate-limited away. A sweep of two seeds should
                // still show that it happened.
                last_drawn: Instant::now() - FRAME,
                dirty: false,
            })
        });

        Self { style, state }
    }

    /// Redraws, unless it is too soon or the last run just finished.
    ///
    /// The final update always draws, whatever the frame budget says, so the
    /// bar is never left reading 9,998 of 10,000 for the moment before it is
    /// erased.
    pub fn update(&self, progress: Progress) {
        let Some(state) = &self.state else {
            return;
        };

        let Ok(mut state) = state.lock() else {
            return;
        };

        let last = progress.completed >= progress.total;

        if !last && state.last_drawn.elapsed() < FRAME {
            return;
        }

        state.last_drawn = Instant::now();
        state.dirty = true;

        let mut stderr = std::io::stderr().lock();
        let _ = write!(stderr, "\r\x1b[2K{}", self.render(progress));
        let _ = stderr.flush();
    }

    /// Draws a named phase with a simple done-of-total.
    ///
    /// Shrinking is the other long wait in a sweep and the one nobody expects:
    /// the seed bar reaches 400/400, and then the tool sits there re-running
    /// each failure dozens of times with nothing on screen. A sweep that
    /// appears finished and then does not return for a minute is one people
    /// interrupt.
    pub fn phase(&self, label: &str, done: u64, total: u64, elapsed: Duration) {
        let Some(state) = &self.state else {
            return;
        };

        let Ok(mut state) = state.lock() else {
            return;
        };

        let last = done >= total;

        if !last && state.last_drawn.elapsed() < FRAME {
            return;
        }

        state.last_drawn = Instant::now();
        state.dirty = true;

        let line = format!(
            "  {} {}  {}  {}",
            self.style.paint(self.style.warn, self.bar(done, total)),
            self.style.paint(self.style.dim, format!("{done}/{total}")),
            self.style.paint(self.style.dim, label),
            self.style
                .paint(self.style.dim, format!("{:.1}s", elapsed.as_secs_f64()))
        );

        let mut stderr = std::io::stderr().lock();
        let _ = write!(stderr, "\r\x1b[2K{line}");
        let _ = stderr.flush();
    }

    /// Erases the bar, so whatever prints next starts on a clean line.
    ///
    /// Erased rather than left in place. A finished bar reading 400/400 says
    /// nothing the summary underneath it does not say better, and leaving it
    /// would put a line of decoration between the command and its answer.
    pub fn finish(&self) {
        let Some(state) = &self.state else {
            return;
        };

        let Ok(mut state) = state.lock() else {
            return;
        };

        if !state.dirty {
            return;
        }

        state.dirty = false;

        let mut stderr = std::io::stderr().lock();
        let _ = write!(stderr, "\r\x1b[2K");
        let _ = stderr.flush();
    }

    /// The bar itself, filled in proportion.
    fn bar(&self, done: u64, total: u64) -> String {
        let filled = (done * WIDTH as u64 / total.max(1)) as usize;

        format!(
            "{}{}",
            "\u{2588}".repeat(filled.min(WIDTH)),
            "\u{2591}".repeat(WIDTH.saturating_sub(filled))
        )
    }

    fn render(&self, progress: Progress) -> String {
        let bar = self.bar(progress.completed, progress.total);

        // Green while nothing has broken, red once something has. The bar is
        // the only thing on screen during a long sweep, so it is where "this
        // is finding things" should be visible without reading a number.
        let colour = if progress.failing > 0 {
            self.style.bad
        } else {
            self.style.good
        };

        let found = if progress.failing > 0 {
            self.style
                .paint(self.style.bad, format!("{} failing", progress.failing))
        } else {
            self.style.paint(self.style.dim, "none failing")
        };

        format!(
            "  {} {}  {}  {}",
            self.style.paint(colour, bar),
            self.style.paint(
                self.style.dim,
                format!("{}/{}", progress.completed, progress.total)
            ),
            found,
            self.style.paint(
                self.style.dim,
                format!("{:.1}s", progress.elapsed.as_secs_f64())
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(completed: u64, total: u64, failing: u64) -> Progress {
        Progress {
            completed,
            total,
            failing,
            elapsed: Duration::from_secs(1),
        }
    }

    fn bar() -> Bar {
        Bar {
            style: Style::plain(),
            state: None,
        }
    }

    #[test]
    fn the_bar_fills_in_proportion() {
        let half = bar().render(progress(50, 100, 0));

        assert_eq!(half.matches('\u{2588}').count(), WIDTH / 2);
        assert_eq!(half.matches('\u{2591}').count(), WIDTH / 2);
    }

    /// The last frame is a full bar and nothing else. An off-by-one here shows
    /// up as a sweep that never quite finishes.
    #[test]
    fn a_finished_sweep_fills_the_bar_completely() {
        let done = bar().render(progress(400, 400, 3));

        assert_eq!(done.matches('\u{2588}').count(), WIDTH);
        assert_eq!(done.matches('\u{2591}').count(), 0);
    }

    /// A sweep of zero seeds is a scenario nobody meant to write, and it must
    /// not divide by it.
    #[test]
    fn an_empty_sweep_does_not_divide_by_zero() {
        assert!(!bar().render(progress(0, 0, 0)).is_empty());
    }

    #[test]
    fn a_clean_sweep_says_none_failing() {
        assert!(bar().render(progress(10, 100, 0)).contains("none failing"));
    }

    #[test]
    fn failures_are_counted_where_someone_will_see_them() {
        assert!(bar().render(progress(10, 100, 4)).contains("4 failing"));
    }

    /// The two phases share one bar, so the fill maths has to be the same one.
    #[test]
    fn a_phase_fills_the_same_way_the_sweep_does() {
        assert_eq!(bar().bar(1, 4), bar().bar(25, 100));
    }

    /// Shrinking one failure of one is a full bar, not a division by zero and
    /// not an empty one.
    #[test]
    fn a_single_item_phase_completes() {
        assert_eq!(bar().bar(1, 1).matches('\u{2588}').count(), WIDTH);
        assert_eq!(bar().bar(0, 0).matches('\u{2591}').count(), WIDTH);
    }

    /// Nothing is written when stderr is not a terminal, and `finish` on a bar
    /// that never drew must not emit a stray erase.
    #[test]
    fn a_bar_that_does_not_draw_does_nothing() {
        let quiet = bar();

        quiet.update(progress(1, 10, 0));
        quiet.phase("shrinking", 1, 3, Duration::from_secs(1));
        quiet.finish();
    }
}
