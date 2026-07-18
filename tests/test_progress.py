import io
import logging
import unittest
from unittest.mock import patch
import pytest

from mrdl.progress import (
    BuiltinProgress, MultiProgress, _get_unit_and_value, _format_time,
    _resolve_tty, ProgressLogHandler, _visible_len, _pad_line, _ANSI_RE,
)


class TestGetUnitAndValue(unittest.TestCase):
    def test_bytes(self):
        assert _get_unit_and_value(0) == (0, "B")
        assert _get_unit_and_value(512) == (512, "B")
        assert _get_unit_and_value(1023) == (1023, "B")

    def test_kibibytes(self):
        assert _get_unit_and_value(1024) == (1.0, "KiB")
        assert _get_unit_and_value(1536) == (1.5, "KiB")

    def test_mebibytes(self):
        assert _get_unit_and_value(1024 * 1024) == (1.0, "MiB")
        assert _get_unit_and_value(int(2.5 * 1024 * 1024)) == (2.5, "MiB")

    def test_gibibytes(self):
        assert _get_unit_and_value(1024 * 1024 * 1024) == (1.0, "GiB")


class TestFormatTime(unittest.TestCase):
    def test_seconds_only(self):
        assert _format_time(45) == "00:45"

    def test_minutes_and_seconds(self):
        assert _format_time(125) == "02:05"

    def test_hours(self):
        assert _format_time(3661) == "1:01:01"

    def test_negative(self):
        assert _format_time(-1) == "--:--"

    def test_infinity(self):
        assert _format_time(float("inf")) == "--:--"


class TestBuiltinProgress(unittest.TestCase):
    def setUp(self):
        self.term_patcher = patch("mrdl.progress._get_term_width", return_value=120)
        self.term_patcher.start()

    def tearDown(self):
        self.term_patcher.stop()

    def test_start_renders_to_stderr(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress = BuiltinProgress()
            progress.start(
                total_bytes=1000,
                filename="test.bin",
                chunk_size=100,
            )

        output = stderr.getvalue()
        assert "test.bin" in output
        assert "0.0%" in output

    def test_update_advances_progress(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress = BuiltinProgress()
            progress._refresh_interval = 0.0
            progress.start(
                total_bytes=1000,
                filename="test.bin",
                chunk_size=500,
            )
            progress.update(500, chunk_index=0)

        output = stderr.getvalue()
        assert "test.bin" in output

    def test_update_with_chunk_index_tracks_chunks(self):
        progress = BuiltinProgress()
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress.start(
                total_bytes=1000,
                filename="test.bin",
                chunk_size=250,
            )
            progress.update(250, chunk_index=2)

        assert 2 in progress._state.completed_chunks
        assert 0 not in progress._state.completed_chunks

    def test_close_writes_newline(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress = BuiltinProgress()
            progress.start(
                total_bytes=1000,
                filename="test.bin",
                chunk_size=500,
            )
            progress.close()

        output = stderr.getvalue()
        assert output.endswith("\n")

    def test_update_before_start_is_safe(self):
        progress = BuiltinProgress()
        progress.update(100, chunk_index=0)
        assert progress._state.completed_bytes == 100

    def test_close_before_start_is_safe(self):
        progress = BuiltinProgress()
        progress.close()
        assert progress._state.started is False

    def test_resume_with_completed_chunks(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress = BuiltinProgress()
            progress.start(
                total_bytes=1000,
                filename="test.bin",
                chunk_size=250,
                completed_chunks={0, 1},
            )

        assert progress._state.completed_chunks == {0, 1}
        assert progress._state.completed_bytes == 500

    def test_full_completion_shows_100_percent(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress = BuiltinProgress()
            progress._refresh_interval = 0.0
            progress.start(
                total_bytes=400,
                filename="test.bin",
                chunk_size=100,
            )
            for i in range(4):
                progress.update(100, chunk_index=i)
            progress._render(force=True)

        output = stderr.getvalue()
        assert "100.0%" in output
        progress.close()

    def test_update_hashed_tracks_chunks(self):
        progress = BuiltinProgress()
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress.start(
                total_bytes=1000,
                filename="test.bin",
                chunk_size=250,
            )
            progress.update(250, chunk_index=0)
            progress.update_hashed(0)

        assert 0 in progress._state.hashed_chunks
        assert 1 not in progress._state.hashed_chunks

    def test_update_hashed_before_start_is_safe(self):
        progress = BuiltinProgress()
        progress.update_hashed(0)
        assert 0 in progress._state.hashed_chunks

    def test_hashed_chunks_render_green(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress = BuiltinProgress()
            progress._refresh_interval = 0.0
            progress.start(
                total_bytes=400,
                filename="test.bin",
                chunk_size=100,
            )
            for i in range(4):
                progress.update(100, chunk_index=i)
            for i in range(4):
                progress.update_hashed(i)
            progress._render(force=True)

        output = stderr.getvalue()
        assert "\033[32m" in output
        progress.close()

    def test_downloaded_not_hashed_renders_blue(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress = BuiltinProgress()
            progress._refresh_interval = 0.0
            progress.start(
                total_bytes=400,
                filename="test.bin",
                chunk_size=100,
            )
            for i in range(4):
                progress.update(100, chunk_index=i)
            progress._render(force=True)

        output = stderr.getvalue()
        assert "\033[34m" in output
        progress.close()

    def test_speed_calculation_with_resume(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr), patch("time.monotonic") as mock_time, patch.object(BuiltinProgress, "_spinner_loop"):
            mock_time.return_value = 100.0

            progress = BuiltinProgress(speed_ema_window=2.0)
            progress._refresh_interval = 0.0
            progress.start(
                total_bytes=1000,
                filename="test.bin",
                chunk_size=250,
                completed_chunks={0},
            )

            output = stderr.getvalue()
            assert "   0.00   B/s" in output

            stderr.seek(0)
            stderr.truncate(0)

            mock_time.return_value = 101.0
            progress.update(100)
            progress._render(force=True)

            output = stderr.getvalue()
            assert " 100.00   B/s" in output

            stderr.seek(0)
            stderr.truncate(0)

            mock_time.return_value = 102.0
            progress.update(150)
            progress._render(force=True)
    
            output = stderr.getvalue()
            assert " 131.12   B/s" in output

            stderr.seek(0)
            stderr.truncate(0)

            mock_time.return_value = 105.0
            progress.update(100)
            progress._render(force=True)
            output = stderr.getvalue()
            assert "  48.36   B/s" in output
            
            progress.close()

    def test_unknown_size_updates_downloaded_amount(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            progress = BuiltinProgress()
            progress._refresh_interval = 0.0
            progress.start(
                total_bytes=0,
                filename="test.bin",
                chunk_size=100,
            )
            progress.update(500)
            line = progress.render_line(120)
            assert "500.00" in line



class TestMultiProgress(unittest.TestCase):
    def setUp(self):
        self.term_patcher = patch("mrdl.progress._get_term_width", return_value=120)
        self.term_patcher.start()

    def tearDown(self):
        self.term_patcher.stop()

    def test_add_bar(self):
        mp = MultiProgress()
        bar1 = mp.add_bar()
        assert isinstance(bar1, BuiltinProgress)
        assert bar1 in mp._bars

    def test_rendering_stacked(self):
        stream = io.StringIO()
        mp = MultiProgress(file=stream, force_tty=True)
        bar1 = mp.add_bar()
        bar2 = mp.add_bar()

        bar1._refresh_interval = 0.0
        bar2._refresh_interval = 0.0
        mp._refresh_interval = 0.0

        bar1.start(1000, "file1.bin", 100)
        bar2.start(2000, "file2.bin", 200)

        output = stream.getvalue()
        assert "file1.bin" in output
        assert "file2.bin" in output

        stream.seek(0)
        stream.truncate(0)

        bar1.update(100)
        mp._render(force=True)
        output_update = stream.getvalue()
        assert "\033[2A" in output_update
        mp.close()

    def test_close(self):
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            mp = MultiProgress()
            bar1 = mp.add_bar()
            bar1._refresh_interval = 0.0
            mp._refresh_interval = 0.0

            bar1.start(1000, "file1.bin", 100)
            mp.close()

            assert mp._is_closed

    def _strip_ansi(self, s: str) -> str:
        """Strips ANSI escape sequences from a string for visual-length checks."""
        return _ANSI_RE.sub("", s)

    def _make_started_bar(self, filename: str, total: int = 1024 * 1024 * 100,
                          chunk_size: int = 1024 * 1024) -> BuiltinProgress:
        """Helper: creates and starts a standalone BuiltinProgress bar."""
        bar = BuiltinProgress()
        bar._refresh_interval = 0.0
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            bar.start(total, filename, chunk_size, completed_chunks={i for i in range(50)})
        return bar

    def test_bar_widths_are_equal_for_different_filename_lengths(self):
        """Lines rendered with the same filename_width must have the same visual length."""
        short_name = "ubuntu-desktop.iso"          # 18 chars
        long_name  = "ubuntu-live-server-amd64.iso"  # 28 chars
        max_len = max(len(short_name), len(long_name))

        bar_short = self._make_started_bar(short_name)
        bar_long  = self._make_started_bar(long_name)

        line_short = self._strip_ansi(bar_short.render_line(120, filename_width=max_len))
        line_long  = self._strip_ansi(bar_long.render_line(120, filename_width=max_len))

        assert len(line_short) == len(line_long), (
            f"Line lengths differ: {len(line_short)} vs {len(line_long)}\n"
            f"  short: {line_short!r}\n"
            f"  long:  {line_long!r}"
        )

    def test_shorter_filename_padded_so_pipe_aligns(self):
        """The '|' delimiter must appear at the same column for both bars."""
        short_name = "ubuntu-desktop.iso"           # 18 chars
        long_name  = "ubuntu-live-server-amd64.iso"  # 28 chars
        max_len = max(len(short_name), len(long_name))

        bar_short = self._make_started_bar(short_name)
        bar_long  = self._make_started_bar(long_name)

        line_short = self._strip_ansi(bar_short.render_line(120, filename_width=max_len))
        line_long  = self._strip_ansi(bar_long.render_line(120, filename_width=max_len))

        pipe_col_short = line_short.index("|")
        pipe_col_long  = line_long.index("|")

        assert pipe_col_short == pipe_col_long, (
            f"'|' columns differ: {pipe_col_short} vs {pipe_col_long}"
        )

    def test_standalone_bar_unaffected_by_default(self):
        """Without filename_width the line is the same as before (no spurious padding)."""
        bar = self._make_started_bar("short.iso")
        line_with    = self._strip_ansi(bar.render_line(120, filename_width=len("short.iso")))
        line_without = self._strip_ansi(bar.render_line(120))

        # Both should produce the same output when filename_width equals the actual name len
        assert line_with == line_without

    def test_filename_width_larger_than_space_clips_safely(self):
        """A very large filename_width must not crash or produce a line longer than term_width."""
        bar = self._make_started_bar("short.iso")
        term_width = 80
        line = self._strip_ansi(bar.render_line(term_width, filename_width=9999))
        # The rendered line must not exceed the terminal width
        assert len(line) <= term_width, (
            f"Line too long ({len(line)} > {term_width}): {line!r}"
        )


class TestDeadlockPrevention(unittest.TestCase):
    """Tests ensuring that concurrent lock acquisition between
    BuiltinProgress._lock and MultiProgress._lock does not deadlock.

    These tests produce a lock-order inversion scenario: BuiltinProgress.log() 
    used to hold BuiltinProgress._lock while calling MultiProgress.log(), which 
    acquires MultiProgress._lock.  Meanwhile, MultiProgress._render() holds 
    MultiProgress._lock and calls bar.render_line(), which acquires 
    BuiltinProgress._lock — a classic ABBA deadlock.
    """

    TIMEOUT = 2  # seconds — generous ceiling; deadlocked threads never finish

    def setUp(self):
        self.term_patcher = patch("mrdl.progress._get_term_width", return_value=120)
        self.term_patcher.start()

    def tearDown(self):
        self.term_patcher.stop()

    def test_log_callback_invoked_without_holding_child_lock(self):
        """The log callback must be called *after* BuiltinProgress._lock is released."""
        import threading

        lock_was_held = [True]
        progress = BuiltinProgress()

        def spy_callback(msg):
            # If the lock is held by the current thread, acquire() would block
            # on a regular Lock. We try a non-blocking acquire; if it succeeds
            # the lock was NOT held (good).
            acquired = progress._lock.acquire(blocking=False)
            if acquired:
                progress._lock.release()
                lock_was_held[0] = False
            else:
                lock_was_held[0] = True

        progress._log_callback = spy_callback
        progress.log("test message")

        assert not lock_was_held[0], (
            "BuiltinProgress._lock was still held when the log callback was "
            "invoked — this will deadlock when the callback acquires "
            "MultiProgress._lock"
        )

    def test_concurrent_log_and_render_does_not_deadlock(self):
        """Hammers concurrent log() + _render() to surface lock-order inversions.

        Thread A: calls bar.log() repeatedly (child → coordinator path)
        Thread B: calls mp._render() repeatedly (coordinator → child path)
        """
        import threading

        stderr = io.StringIO()
        iterations = 200

        with patch("sys.stderr", stderr):
            mp = MultiProgress()
            mp._refresh_interval = 0.0
            bar = mp.add_bar()
            bar._refresh_interval = 0.0
            bar.start(
                total_bytes=10_000,
                filename="deadlock_test.bin",
                chunk_size=1000,
            )

            errors: list[Exception] = []

            def log_worker():
                try:
                    for i in range(iterations):
                        bar.log(f"log message {i}")
                except Exception as exc:
                    errors.append(exc)

            def render_worker():
                try:
                    for _ in range(iterations):
                        mp._render(force=True)
                except Exception as exc:
                    errors.append(exc)

            t_log = threading.Thread(target=log_worker)
            t_render = threading.Thread(target=render_worker)

            t_log.start()
            t_render.start()

            t_log.join(timeout=self.TIMEOUT)
            t_render.join(timeout=self.TIMEOUT)

            alive = [t for t in (t_log, t_render) if t.is_alive()]
            assert not alive, (
                f"Deadlock detected: {len(alive)} thread(s) still alive after "
                f"{self.TIMEOUT}s timeout"
            )
            assert not errors, f"Unexpected errors: {errors}"

            bar.close()
            mp.close()

    def test_spinner_thread_and_log_do_not_deadlock(self):
        """Reproduces a previous bug scenario: a spinner thread running 
        _render while the main thread calls bar.log().

        When total_bytes=0, BuiltinProgress.start() spawns a background spinner
        thread that repeatedly calls _render().  If log() held the child lock
        while invoking the MultiProgress callback, these two threads would
        deadlock.
        """
        import threading

        stderr = io.StringIO()
        iterations = 100

        with patch("sys.stderr", stderr):
            mp = MultiProgress()
            mp._refresh_interval = 0.0
            bar = mp.add_bar()
            bar._refresh_interval = 0.0

            # total_bytes=0 triggers the spinner thread
            bar.start(
                total_bytes=0,
                filename="spinner_deadlock.bin",
                chunk_size=1024,
            )

            errors: list[Exception] = []

            def log_burst():
                try:
                    for i in range(iterations):
                        bar.log(f"warning {i}")
                except Exception as exc:
                    errors.append(exc)

            t = threading.Thread(target=log_burst)
            t.start()
            t.join(timeout=self.TIMEOUT)

            assert not t.is_alive(), (
                f"Deadlock detected: log thread still alive after {self.TIMEOUT}s "
                f"(spinner thread is running concurrently)"
            )
            assert not errors, f"Unexpected errors: {errors}"

            bar.close()
            mp.close()


class TestResolveTTY(unittest.TestCase):
    def test_explicit_true_overrides_everything(self):
        """force_tty=True should return True even if stream is not a TTY."""
        stream = io.StringIO()  # isatty() would return False
        assert _resolve_tty(stream, force_tty=True) is True

    def test_explicit_false_overrides_everything(self):
        """force_tty=False should return False even if env says otherwise."""
        with patch.dict("os.environ", {"FORCE_TTY": "1"}):
            stream = io.StringIO()
            assert _resolve_tty(stream, force_tty=False) is False

    def test_env_force_tty_respected(self):
        """FORCE_TTY=1 should enable TTY mode."""
        with patch.dict("os.environ", {"FORCE_TTY": "1"}, clear=False):
            stream = io.StringIO()
            assert _resolve_tty(stream, force_tty=None) is True

    def test_env_force_color_respected(self):
        """FORCE_COLOR=1 should enable TTY mode when FORCE_TTY is not set."""
        env = {"FORCE_COLOR": "1"}
        with patch.dict("os.environ", env, clear=True):
            stream = io.StringIO()
            assert _resolve_tty(stream, force_tty=None) is True

    def test_force_tty_takes_precedence_over_force_color(self):
        """FORCE_TTY should take precedence over FORCE_COLOR."""
        env = {"FORCE_TTY": "0", "FORCE_COLOR": "1"}
        with patch.dict("os.environ", env, clear=True):
            stream = io.StringIO()
            assert _resolve_tty(stream, force_tty=None) is False

    def test_falls_back_to_isatty(self):
        """When no overrides, should use stream.isatty()."""
        stream = io.StringIO()
        with patch.dict("os.environ", {}, clear=True):
            assert _resolve_tty(stream, force_tty=None) is False


class TestForceTTY(unittest.TestCase):
    def setUp(self):
        self.term_patcher = patch("mrdl.progress._get_term_width", return_value=120)
        self.term_patcher.start()

    def tearDown(self):
        self.term_patcher.stop()

    def test_multi_progress_force_tty_enables_ansi(self):
        """force_tty=True should produce ANSI cursor movement on a non-TTY stream."""
        stream = io.StringIO()
        mp = MultiProgress(file=stream, force_tty=True)
        bar = mp.add_bar()
        bar.start(1000, "test.bin", 100)
        mp._render(force=True)
        mp._render(force=True)  # Second render should produce cursor-up
        output = stream.getvalue()
        assert "\033[" in output  # ANSI escape present
        mp.close()


class TestProgressLogHandler(unittest.TestCase):
    def setUp(self):
        self.term_patcher = patch("mrdl.progress._get_term_width", return_value=120)
        self.term_patcher.start()

    def tearDown(self):
        self.term_patcher.stop()

    def test_handler_routes_log_through_multiprogress(self):
        """Log records should appear via MultiProgress.log()."""
        stream = io.StringIO()
        mp = MultiProgress(file=stream, force_tty=False)
        bar = mp.add_bar()
        bar.start(1000, "test.bin", 100)

        handler = ProgressLogHandler(mp)
        logger = logging.getLogger("test.progress_handler")
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)

        try:
            logger.warning("mirror timeout reached")
            output = stream.getvalue()
            assert "mirror timeout reached" in output
        finally:
            logger.removeHandler(handler)
            mp.close()

    def test_handler_with_custom_formatter(self):
        """Custom formatters should be applied to the log record."""
        stream = io.StringIO()
        mp = MultiProgress(file=stream, force_tty=False)

        handler = ProgressLogHandler(mp)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        logger = logging.getLogger("test.progress_handler_fmt")
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)

        try:
            logger.warning("disk full")
            output = stream.getvalue()
            assert "[WARNING] disk full" in output
        finally:
            logger.removeHandler(handler)
            mp.close()


class TestVisibleLen(unittest.TestCase):
    def test_plain_string(self):
        assert _visible_len("hello") == 5

    def test_ansi_codes_ignored(self):
        assert _visible_len("\033[32mhello\033[0m") == 5

    def test_empty_string(self):
        assert _visible_len("") == 0

    def test_multiple_ansi_codes(self):
        s = "\033[34m\033[1mfoo\033[0m bar"
        assert _visible_len(s) == 7  # "foo bar"


class TestPadLine(unittest.TestCase):
    def test_pads_short_line(self):
        result = _pad_line("hello", 10)
        assert result == "hello     "
        assert len(result) == 10

    def test_no_padding_when_exact(self):
        result = _pad_line("hello", 5)
        assert result == "hello"

    def test_no_padding_when_longer(self):
        result = _pad_line("hello world", 5)
        assert result == "hello world"

    def test_ansi_aware_padding(self):
        line = "\033[32mhi\033[0m"  # visible: "hi" = 2 chars
        result = _pad_line(line, 6)
        # Should pad based on visible length (2), not string length
        assert _visible_len(result) == 6


class TestNoEraseInLine(unittest.TestCase):
    def setUp(self):
        self.term_patcher = patch("mrdl.progress._get_term_width", return_value=120)
        self.term_patcher.start()

    def tearDown(self):
        self.term_patcher.stop()

    def test_no_erase_in_line_in_output(self):
        """Output should not contain \\033[K (Erase in Line) sequences."""
        stream = io.StringIO()
        mp = MultiProgress(file=stream, force_tty=True)
        bar = mp.add_bar()
        bar.start(1000, "test.bin", 100)
        mp._render(force=True)
        mp._render(force=True)
        output = stream.getvalue()
        assert "\033[K" not in output
        mp.close()
