from __future__ import annotations

import math
import shutil
import sys
import threading
import time
import collections
from dataclasses import dataclass
from typing import Callable, Literal


# ANSI color codes
GREEN = "\033[32m"
BLUE = "\033[34m"
GRAY = "\033[90m"
RESET = "\033[0m"

# Unicode block characters for shading levels
BLOCK_FULL = "\u2588"
BLOCK_HIGH = "\u2593"
BLOCK_MED = "\u2592"
BLOCK_LOW = "\u2591"


def _get_unit_and_value(n: float) -> tuple[float, str]:
    """Returns the scaled value and unit for a byte count."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return n, unit
        n /= 1024
    return n, "PiB"


def _format_time(seconds: float) -> str:
    """Formats seconds into a compact time string.

    Args:
        seconds: Duration in seconds.

    Returns:
        A formatted string like '1:23:45' or '12:34'.
    """
    if seconds < 0 or not math.isfinite(seconds):
        return "--:--"
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _truncate_filename(filename: str, max_len: int) -> str:
    """Truncates a filename to max_len by keeping the beginning and end, and inserting '...' in the middle."""
    if len(filename) <= max_len:
        return filename
    if max_len <= 5:
        return filename[:max_len]
    keep = (max_len - 3) // 2
    prefix = filename[:keep]
    suffix = filename[-(max_len - 3 - keep):]
    return f"{prefix}...{suffix}"


def _get_term_width() -> int:
    """Returns the current terminal width or a default of 80."""
    try:
        return shutil.get_terminal_size().columns
    except (AttributeError, ValueError, OSError):
        return 80


@dataclass
class ProgressState:
    total_bytes: int
    completed_bytes: int
    total_chunks: int
    completed_chunks: set[int]
    hashed_chunks: set[int]
    filename: str
    mode: Literal["download", "verify"]
    compact: bool
    overlay_text: str
    overlay_success: bool
    overlay_color: str | None
    is_throttled: bool
    speed: float
    elapsed: float
    eta: float
    now: float
    has_started: bool
    started: bool


class ProgressFormatter:
    """Handles the presentation and layout logic for the progress bar."""
    
    SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def __init__(self) -> None:
        import random
        self._spinner_offset = random.randint(0, len(self.SPINNER_FRAMES) - 1)

    _COLOR_MAP = {
        "red": "\033[41;30m",
        "yellow": "\033[43;30m",
        "blue": "\033[44;30m",
        "green": "\033[42;30m",
    }
    
    _BLOCKS = [BLOCK_LOW, BLOCK_MED, BLOCK_HIGH, BLOCK_FULL]

    def _get_overlay_bg_color(self, state: ProgressState) -> str:
        if state.overlay_color and state.overlay_color in self._COLOR_MAP:
            return self._COLOR_MAP[state.overlay_color]
        if not state.overlay_success:
            return self._COLOR_MAP["red"]
        if "HASH OK" in state.overlay_text:
            return self._COLOR_MAP["blue"]
        return self._COLOR_MAP["green"]

    def _format_speed(self, state: ProgressState, padded: bool = False) -> tuple[str, str]:
        speed_val, speed_unit = _get_unit_and_value(state.speed)
        if padded:
            visible_speed_str = f" {speed_val:7.2f} {speed_unit:>3}/s"
            if state.is_throttled:
                return visible_speed_str, f" \033[31m{visible_speed_str.strip()}\033[0m"
            return visible_speed_str, visible_speed_str
        else:
            visible_speed_str = f"{speed_val:.2f} {speed_unit}/s"
            if state.is_throttled:
                return visible_speed_str, f"\033[31m{visible_speed_str}\033[0m"
            return visible_speed_str, visible_speed_str

    def _format_size(self, state: ProgressState, padded: bool = False) -> str:
        if state.total_bytes > 0:
            t_val, t_unit = _get_unit_and_value(state.total_bytes)
            c_val = state.completed_bytes
            for u in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
                if u == t_unit:
                    break
                c_val /= 1024.0
            t_width = len(f"{t_val:.2f}")
            return f"{c_val:{t_width}.2f}/{t_val:{t_width}.2f} {t_unit:>3}"
        else:
            c_val, c_unit = _get_unit_and_value(state.completed_bytes)
            if padded:
                return f"{c_val:7.2f} {c_unit:>3}"
            else:
                return f"{c_val:.2f} {c_unit}"

    def _get_spinner_char(self, state: ProgressState) -> str:
        if not state.has_started:
            return " "
        elif state.overlay_text and ("PAUSED" in state.overlay_text or "STATE SAVED" in state.overlay_text):
            return "⏸"
        elif not state.started and state.overlay_text:
            return "✓" if state.overlay_success else "✗"
        frame_idx = (int(state.now * 10) + self._spinner_offset) % len(self.SPINNER_FRAMES)
        return self.SPINNER_FRAMES[frame_idx]

    def _build_bar(self, state: ProgressState, width: int) -> str:
        if state.total_chunks == 0 or width == 0:
            return GRAY + (BLOCK_LOW * width)

        completed_arr = [0.0] * width
        hashed_arr = [0.0] * width

        for col in range(width):
            start_chunk_frac = col * state.total_chunks / width
            end_chunk_frac = (col + 1) * state.total_chunks / width
            
            start_chunk = int(start_chunk_frac)
            end_chunk = math.ceil(end_chunk_frac)
            
            if end_chunk - start_chunk == 1:
                chunk_idx = start_chunk
                if chunk_idx in state.hashed_chunks:
                    hashed_arr[col] = 1.0
                if chunk_idx in state.completed_chunks:
                    completed_arr[col] = 1.0
            else:
                total_in_col = 0
                completed_in_col = 0
                hashed_in_col = 0
                for c in range(start_chunk, end_chunk):
                    total_in_col += 1
                    if c in state.completed_chunks:
                        completed_in_col += 1
                    if c in state.hashed_chunks:
                        hashed_in_col += 1
                if total_in_col > 0:
                    completed_arr[col] = completed_in_col / total_in_col
                    hashed_arr[col] = hashed_in_col / total_in_col

        chars: list[str] = []
        for i in range(width):
            if hashed_arr[i] > 0:
                idx = math.ceil(hashed_arr[i] * 3)
                chars.append(GREEN + self._BLOCKS[idx])
            elif completed_arr[i] > 0:
                idx = math.ceil(completed_arr[i] * 3)
                chars.append(BLUE + self._BLOCKS[idx])
            else:
                chars.append(GRAY + BLOCK_LOW)

        if state.overlay_text and len(state.overlay_text) <= width:
            text_len = len(state.overlay_text)
            start_idx = (width - text_len) // 2
            bg_color = self._get_overlay_bg_color(state)
            for i, char in enumerate(state.overlay_text):
                chars[start_idx + i] = f"{bg_color}{char}{RESET}"

        return "".join(chars)

    def _render_compact(self, state: ProgressState, term_width: int, filename_width: int | None) -> str:
        spinner_char = self._get_spinner_char(state)

        if spinner_char == "✓":
            spinner_char_formatted = f"{GREEN}✓{RESET}"
        elif spinner_char == "✗":
            spinner_char_formatted = f"\033[31m✗{RESET}"
        elif spinner_char == "⏸":
            spinner_char_formatted = f"\033[33m⏸{RESET}"
        else:
            spinner_char_formatted = spinner_char
            
        prefix = f"{spinner_char_formatted} "
        visible_prefix = 2

        if state.overlay_text:
            bg_color = self._get_overlay_bg_color(state)
            status_suffix = f" | {bg_color}{state.overlay_text}{RESET}"
            visible_right = len(state.overlay_text) + 3
            formatted_right = status_suffix
        else:
            visible_speed_str, speed_str = self._format_speed(state, padded=False)
            size_str = self._format_size(state, padded=False)
                
            stats_visible = f" | {size_str} @ {visible_speed_str}"
            stats_formatted = f" | {size_str} @ {speed_str}"
            visible_right = len(stats_visible)
            formatted_right = stats_formatted

        needed_space = visible_prefix + visible_right
        effective_name_len = filename_width if filename_width is not None else len(state.filename)
        space_for_file = term_width - needed_space
        
        if space_for_file >= 5:
            filename_len = min(effective_name_len, space_for_file)
            display_filename = _truncate_filename(state.filename, filename_len)
            display_filename = display_filename.ljust(filename_len)
            return f"{prefix}{display_filename}{formatted_right}"
        else:
            return f"{prefix}{formatted_right}"[:term_width]

    def _render_unknown_size(self, state: ProgressState, term_width: int, filename_width: int | None) -> str:
        frame_idx = (int(state.now * 10) + self._spinner_offset) % len(self.SPINNER_FRAMES)
        spinner_char = self.SPINNER_FRAMES[frame_idx]
        
        size_str = self._format_size(state, padded=False)
        visible_speed_str, speed_str = self._format_speed(state, padded=False)
        
        stats_visible = f" | {size_str} @ {visible_speed_str}"
        stats_formatted = f" | {size_str} @ {speed_str}"
        
        if state.overlay_text:
            bg_color = self._get_overlay_bg_color(state)
            status_suffix = f" {bg_color}{state.overlay_text}{RESET}"
            status_len = len(state.overlay_text) + 1
        else:
            status_suffix = f" {spinner_char} downloading (progress unavailable)"
            status_len = len(status_suffix)
        
        needed_space = status_len + len(stats_visible)
        
        if term_width <= len(stats_visible):
            return stats_visible[:term_width]
        
        filename_space = term_width - needed_space
        if filename_space >= 10:
            display_filename = _truncate_filename(state.filename, filename_space)
            return f"{display_filename}{status_suffix}{stats_formatted}"
        elif term_width >= needed_space + 4:
            display_filename = _truncate_filename(state.filename, term_width - needed_space)
            return f"{display_filename}{status_suffix}{stats_formatted}"
        else:
            if state.overlay_text:
                display_filename = _truncate_filename(state.filename, max(0, term_width - status_len))
                return f"{display_filename}{status_suffix}"
            else:
                simple_status = f" {spinner_char} downloading"
                simple_needed = len(simple_status) + len(stats_visible)
                filename_space = term_width - simple_needed
                if filename_space >= 5:
                    display_filename = _truncate_filename(state.filename, filename_space)
                    return f"{display_filename}{simple_status}{stats_formatted}"
                else:
                    simple_status = f" {spinner_char}"
                    filename_space = term_width - len(simple_status) - len(stats_visible)
                    if filename_space > 0:
                        display_filename = _truncate_filename(state.filename, filename_space)
                        return f"{display_filename}{simple_status}{stats_formatted}"
                    else:
                        return stats_visible[:term_width]

    def _render_standard(self, state: ProgressState, term_width: int, filename_width: int | None) -> str:
        if state.total_bytes > 0:
            pct = (state.completed_bytes / state.total_bytes * 100)
        else:
            pct = 0.0
            
        visible_speed_str, speed_str = self._format_speed(state, padded=True)

        if state.mode == "verify":
            stats = f" {pct:5.1f}% ETA {_format_time(state.eta)}"
            stats_len = len(stats)
        elif state.total_bytes > 0:
            size_str = self._format_size(state, padded=True)
            stats = (
                f"{pct:5.1f}%"
                f" {size_str}"
                f"{speed_str}"
                f" ETA {_format_time(state.eta)}"
            )
            stats_len = len(f"{pct:5.1f}% {size_str}{visible_speed_str} ETA {_format_time(state.eta)}")
        else:
            size_str = self._format_size(state, padded=True)
            stats = (
                f" {size_str}"
                f"{speed_str}"
            )
            stats_len = len(f" {size_str}{visible_speed_str}")

        if term_width <= stats_len:
            return stats[:term_width]

        label_prefix = ""
        rem_width = term_width - stats_len

        if rem_width <= len(label_prefix):
            return (label_prefix + state.filename)[:rem_width]

        space_for_file_and_bar = rem_width - len(label_prefix)

        min_bar_len = 10
        min_filename_len = 10
        needed_filename_len = min(len(state.filename), min_filename_len)
        min_needed_space = needed_filename_len + 4 + min_bar_len

        if space_for_file_and_bar >= min_needed_space and state.total_bytes > 0 and state.total_chunks > 1:
            effective_name_len = filename_width if filename_width is not None else len(state.filename)
            filename_len = min(effective_name_len, space_for_file_and_bar - 4 - min_bar_len)

            bar_width = space_for_file_and_bar - filename_len - 4

            display_filename = state.filename
            if len(display_filename) > filename_len:
                display_filename = _truncate_filename(display_filename, filename_len)
                filename_len = len(display_filename)
                bar_width = space_for_file_and_bar - filename_len - 4

            display_filename = display_filename.ljust(filename_len)

            label = f"{label_prefix}{display_filename}"
            bar = self._build_bar(state, bar_width)
            line = f"{label} |{bar}{RESET}| {stats}"
        else:
            filename_len = space_for_file_and_bar
            display_filename = state.filename
            if len(display_filename) > filename_len:
                display_filename = _truncate_filename(display_filename, filename_len)
            line = f"{label_prefix}{display_filename}{stats}"

        return line

    def render(self, state: ProgressState, term_width: int, filename_width: int | None = None) -> str:
        if state.compact:
            return self._render_compact(state, term_width, filename_width)
        elif state.total_bytes == 0:
            return self._render_unknown_size(state, term_width, filename_width)
        else:
            return self._render_standard(state, term_width, filename_width)


class BuiltinProgress:
    """Thread-safe, chunk-aware progress bar using only Python builtins.

    Visualizes out-of-order chunk completion with Unicode block characters
    and ANSI colors. Adapts dynamically to terminal width changes.
    """

    def __init__(
        self,
        render_callback: Callable[[BuiltinProgress, bool], None] | None = None,
        log_callback: Callable[[str], None] | None = None,
        compact: bool = False,
        speed_ema_window: float = 1.0,
        speed_update_interval: float = 0.2,
    ) -> None:
        """Initializes the BuiltinProgress tracker."""
        self._lock = threading.Lock()
        self._render_callback = render_callback
        self._log_callback = log_callback
        self._speed_ema_window = speed_ema_window
        self._speed_update_interval = speed_update_interval
        self._state = ProgressState(
            total_bytes=0,
            completed_bytes=0,
            total_chunks=0,
            completed_chunks=set(),
            hashed_chunks=set(),
            filename="",
            mode="download",
            compact=compact,
            overlay_text="",
            overlay_success=True,
            overlay_color=None,
            is_throttled=False,
            speed=0.0,
            elapsed=0.0,
            eta=0.0,
            now=0.0,
            has_started=False,
            started=False,
        )
        self._chunk_size = 0
        self._start_time: float | None = None
        self._start_completed_bytes = 0
        self._last_tick_time = 0.0
        self._last_tick_bytes = 0
        self._last_render_time = 0.0
        self._refresh_interval = 0.5
        self._spinner_thread: threading.Thread | None = None
        self._formatter = ProgressFormatter()

    def start(
        self,
        total_bytes: int,
        filename: str,
        chunk_size: int,
        completed_chunks: set[int] | None = None,
        mode: Literal["download", "verify"] = "download",
    ) -> None:
        """Initializes and displays the progress bar.

        Args:
            total_bytes: Total size of the file to download.
            filename: Name of the file being downloaded.
            chunk_size: Size of each download chunk in bytes.
            completed_chunks: Set of already completed chunk indices (for resume).
        """
        was_started = False
        with self._lock:
            self._state.total_bytes = total_bytes
            self._chunk_size = chunk_size
            self._state.filename = filename
            self._state.total_chunks = (total_bytes + chunk_size - 1) // chunk_size if chunk_size > 0 else 0

            if completed_chunks:
                self._state.completed_chunks = set(completed_chunks)
                self._state.completed_bytes = sum(
                    min(chunk_size, total_bytes - (i * chunk_size))
                    for i in self._state.completed_chunks
                )
            else:
                self._state.completed_chunks = set()
                self._state.completed_bytes = 0

            self._start_completed_bytes = self._state.completed_bytes
            self._start_time = time.monotonic()
            self._last_tick_time = self._start_time
            self._last_tick_bytes = self._state.completed_bytes
            self._last_render_time = self._start_time
            was_started = self._state.started
            self._state.started = True
            self._state.has_started = True
            self._state.mode = mode

            if self._render_callback is None and not was_started:
                self._spinner_thread = threading.Thread(target=self._spinner_loop, daemon=True)
                self._spinner_thread.start()
        self._render(force=True)

    def _spinner_loop(self) -> None:
        """Background loop to periodically redraw the progress bar when file size is unknown."""
        while True:
            with self._lock:
                if not self._state.started:
                    break
            self._render(force=True)
            time.sleep(0.1)

    def update(self, bytes_downloaded: int, chunk_index: int | None = None) -> None:
        """Updates the progress bar with newly downloaded bytes.

        Args:
            bytes_downloaded: Number of bytes downloaded since the last update.
            chunk_index: Index of the chunk that was just completed.
        """
        with self._lock:
            self._state.completed_bytes += bytes_downloaded
            if chunk_index is not None:
                self._state.completed_chunks.add(chunk_index)

    def update_hashed(self, chunk_index: int) -> None:
        """Updates the progress bar that a chunk has been verified/hashed.

        Args:
            chunk_index: Index of the chunk that was just hashed.
        """
        with self._lock:
            self._state.hashed_chunks.add(chunk_index)

    def close(self) -> None:
        """Closes the progress bar with a final newline (if standalone)."""
        was_started = False
        with self._lock:
            if self._state.started:
                was_started = True
                self._state.started = False
        if was_started:
            if self._spinner_thread is not None:
                self._spinner_thread.join(timeout=1.0)
                self._spinner_thread = None
            self._render(force=True)
            if self._render_callback is None:
                sys.stderr.write("\n")
                sys.stderr.flush()

    def set_overlay(self, text: str, success: bool = True, color: str | None = None) -> None:
        """Sets the state text to overlay on the progress bar.

        Args:
            text: The text to overlay (e.g., ' HASH OK ').
            success: Whether the state represents success (True) or failure (False).
            color: Optional color override ('red', 'yellow', 'green', 'blue').
        """
        with self._lock:
            self._state.overlay_text = text
            self._state.overlay_success = success
            self._state.overlay_color = color
        self._render(force=True)

    def set_throttled(self, is_throttled: bool) -> None:
        """Sets whether the download is currently throttled."""
        with self._lock:
            self._state.is_throttled = is_throttled
        self._render(force=True)

    def log(self, message: str) -> None:
        """Logs a message safely without breaking the progress bar layout.

        Forwards the message to the coordinator if present, otherwise prints it.

        The callback reference is copied under the lock and then invoked
        *outside* the lock to prevent a lock-order inversion deadlock with
        ``MultiProgress._lock`` (which calls back into ``render_line`` →
        ``BuiltinProgress._lock``).
        """
        callback = None
        with self._lock:
            callback = self._log_callback

        if callback is not None:
            callback(message)
            return

        sys.stdout.write(f"\r\033[K{message}\n")
        sys.stdout.flush()

        with self._lock:
            started = self._state.started
        if started:
            self._render(force=True)

    def _update_dynamic_state(self) -> None:
        total = self._state.total_bytes
        if self._state.mode == "verify":
            completed = len(self._state.hashed_chunks) * self._chunk_size if total == 0 else min(len(self._state.hashed_chunks) * self._chunk_size, total)
        else:
            completed = self._state.completed_bytes if total == 0 else min(self._state.completed_bytes, total)

        now = time.monotonic()
        elapsed = now - self._start_time if self._start_time else 0.0
        
        if self._state.mode == "verify":
            speed = completed / elapsed if elapsed > 0.5 else 0.0
        else:
            dt = now - self._last_tick_time
            
            if dt < self._speed_update_interval:
                speed = self._state.speed
            else:
                delta_bytes = completed - self._last_tick_bytes
                instantaneous_speed = delta_bytes / dt
                self._last_tick_time = now
                self._last_tick_bytes = completed

                session_bytes = completed - self._start_completed_bytes
                
                if self._speed_ema_window <= 0:
                    speed = instantaneous_speed
                elif elapsed < 1.0 or self._state.speed == 0.0:
                    speed = session_bytes / elapsed if elapsed > 0.1 else 0.0
                else:
                    alpha = 1.0 - math.exp(-dt / self._speed_ema_window)
                    speed = alpha * instantaneous_speed + (1.0 - alpha) * self._state.speed
        
        eta = (total - completed) / speed if (speed > 0 and total > 0) else 0.0

        self._state.speed = speed
        self._state.elapsed = elapsed
        self._state.eta = eta
        self._state.now = now

    def render_line(self, term_width: int, filename_width: int | None = None) -> str:
        """Renders the progress bar line contents for a given terminal width."""
        with self._lock:
            self._update_dynamic_state()
            
            total = self._state.total_bytes
            if self._state.mode == "verify":
                display_completed = len(self._state.hashed_chunks) * self._chunk_size if total == 0 else min(len(self._state.hashed_chunks) * self._chunk_size, total)
            else:
                display_completed = self._state.completed_bytes if total == 0 else min(self._state.completed_bytes, total)

            real_completed = self._state.completed_bytes
            self._state.completed_bytes = display_completed
            try:
                return self._formatter.render(self._state, term_width, filename_width)
            finally:
                self._state.completed_bytes = real_completed


    def _render(self, force: bool = False) -> None:
        """Renders the progress bar line to stderr or invokes the callback."""
        now = time.monotonic()
        if not force and now - self._last_render_time < self._refresh_interval:
            return
        self._last_render_time = now

        if self._render_callback is not None:
            self._render_callback(self, force)
            return

        term_width = _get_term_width()

        line = self.render_line(term_width)
        sys.stderr.write(f"\r{line}\033[K")
        sys.stderr.flush()


class MultiProgress:
    """Thread-safe coordinator for rendering multiple progress bars stacked on top of each other."""

    def __init__(self, refresh_interval: float = 0.2, compact: bool = False, speed_ema_window: float = 1.0, speed_update_interval: float = 0.2) -> None:
        self._lock = threading.Lock()
        self._bars: list[BuiltinProgress] = []
        self._last_render_time = 0.0
        self._refresh_interval = refresh_interval
        self._lines_printed = 0
        self._is_closed = False
        self._compact = compact
        self._speed_ema_window = speed_ema_window
        self._speed_update_interval = speed_update_interval
        self._ui_thread = threading.Thread(target=self._ui_loop, daemon=True)
        self._ui_thread.start()

    def _ui_loop(self) -> None:
        while True:
            with self._lock:
                if self._is_closed:
                    break
            self._render(force=True)
            time.sleep(0.1)

    def add_bar(self) -> BuiltinProgress:
        """Adds and returns a new progress bar managed by this coordinator."""
        with self._lock:
            bar = BuiltinProgress(
                render_callback=self._child_update_callback,
                log_callback=self.log,
                compact=self._compact,
                speed_ema_window=self._speed_ema_window,
                speed_update_interval=self._speed_update_interval,
            )
            self._bars.append(bar)
            return bar

    def _child_update_callback(self, child: BuiltinProgress, force: bool = False) -> None:
        """Called by a child progress bar when it wants to trigger a render."""
        self._render(force=force)

    def _draw_lines(self, lines: list[str]) -> None:
        is_tty = sys.stderr.isatty()
        if is_tty:
            if self._lines_printed > 0:
                sys.stderr.write(f"\r\033[{self._lines_printed}A")
            for line in lines:
                sys.stderr.write(f"\r{line}\033[K\n")
            if len(lines) < self._lines_printed:
                for _ in range(self._lines_printed - len(lines)):
                    sys.stderr.write("\r\033[K\n")
                sys.stderr.write(f"\033[{self._lines_printed - len(lines)}A")
            sys.stderr.flush()
            self._lines_printed = len(lines)
        else:
            if lines:
                sys.stderr.write("\n".join(lines) + "\n")
                sys.stderr.flush()

    def _clear_lines(self) -> None:
        is_tty = sys.stderr.isatty()
        if is_tty and self._lines_printed > 0:
            sys.stderr.write(f"\r\033[{self._lines_printed}A")
            for _ in range(self._lines_printed):
                sys.stderr.write("\033[K\n")
            sys.stderr.write(f"\r\033[{self._lines_printed}A")
            sys.stderr.flush()
            self._lines_printed = 0

    def _render(self, force: bool = False) -> None:
        """Renders all managed progress bars, stacking them in order."""
        now = time.monotonic()
        with self._lock:
            if self._is_closed:
                return
            if not force and now - self._last_render_time < self._refresh_interval:
                return
            self._last_render_time = now

            term_width = _get_term_width()
            max_filename_len = max(
                (len(bar._state.filename) for bar in self._bars if bar._state.has_started),
                default=0,
            )

            lines = []
            for bar in self._bars:
                if bar._state.has_started:
                    lines.append(bar.render_line(term_width, filename_width=max_filename_len))

            if not lines:
                return

            self._draw_lines(lines)

    def close(self) -> None:
        """Closes the progress coordinator and prints the final states."""
        with self._lock:
            if self._is_closed:
                return
            self._is_closed = True

            term_width = _get_term_width()
            max_filename_len = max(
                (len(bar._state.filename) for bar in self._bars if bar._state.has_started),
                default=0,
            )

            lines = []
            for bar in self._bars:
                if bar._state.has_started:
                    lines.append(bar.render_line(term_width, filename_width=max_filename_len))

            self._draw_lines(lines)
            self._lines_printed = 0
            
        if hasattr(self, '_ui_thread') and self._ui_thread is not None:
            self._ui_thread.join(timeout=1.0)

    def log(self, message: str) -> None:
        """Logs a message safely by printing it above the stacked progress bars."""
        with self._lock:
            if self._is_closed:
                sys.stdout.write(f"{message}\n")
                sys.stdout.flush()
                return

            term_width = _get_term_width()
            max_filename_len = max(
                (len(bar._state.filename) for bar in self._bars if bar._state.has_started),
                default=0,
            )

            lines = []
            for bar in self._bars:
                if bar._state.has_started:
                    lines.append(bar.render_line(term_width, filename_width=max_filename_len))

            is_tty = sys.stderr.isatty()
            if is_tty:
                self._clear_lines()
                
                msg_lines = message.split("\n")
                for msg_line in msg_lines:
                    sys.stdout.write(f"{msg_line}\n")
                sys.stdout.flush()

                self._draw_lines(lines)
            else:
                sys.stdout.write(f"{message}\n")
                sys.stdout.flush()

class NoOpProgress:
    """A progress reporter that does absolutely nothing.
    
    Useful for daemon processes or CI pipelines where output should be fully suppressed.
    """

    def start(
        self,
        total_bytes: int,
        filename: str,
        chunk_size: int,
        completed_chunks: set[int] | None = None,
        mode: Literal["download", "verify"] = "download",
    ) -> None:
        pass

    def update(self, bytes_downloaded: int, chunk_index: int | None = None) -> None:
        pass

    def update_hashed(self, chunk_index: int) -> None:
        pass

    def close(self) -> None:
        pass

    def log(self, message: str) -> None:
        pass

    def set_overlay(self, text: str, success: bool = True, color: str | None = None) -> None:
        pass

    def set_throttled(self, is_throttled: bool) -> None:
        pass
