from __future__ import annotations

import math
import shutil
import sys
import threading
import time
import collections
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


class BuiltinProgress:
    """Thread-safe, chunk-aware progress bar using only Python builtins.

    Visualizes out-of-order chunk completion with Unicode block characters
    and ANSI colors. Adapts dynamically to terminal width changes.
    """

    def __init__(
        self,
        render_callback: Callable[[BuiltinProgress, bool], None] | None = None,
        log_callback: Callable[[str], None] | None = None,
    ) -> None:
        """Initializes the BuiltinProgress tracker."""
        self._lock = threading.Lock()
        self._render_callback = render_callback
        self._log_callback = log_callback
        self._total_bytes = 0
        self._completed_bytes = 0
        self._chunk_size = 0
        self._total_chunks = 0
        self._completed_chunks: set[int] = set()
        self._hashed_chunks: set[int] = set()
        self._filename = ""
        self._start_time: float | None = None
        self._started = False
        self._has_started = False
        self._overlay_text = ""
        self._overlay_success = True
        self._overlay_color = None
        self._start_completed_bytes = 0
        self._history: collections.deque[tuple[float, int]] = collections.deque()
        self._last_render_time = 0.0
        self._refresh_interval = 0.5
        self._is_throttled = False
        self._mode: Literal["download", "verify"] = "download"
        self._spinner_thread: threading.Thread | None = None

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
            self._total_bytes = total_bytes
            self._chunk_size = chunk_size
            self._filename = filename
            self._total_chunks = (total_bytes + chunk_size - 1) // chunk_size if chunk_size > 0 else 0

            if completed_chunks:
                self._completed_chunks = set(completed_chunks)
                self._completed_bytes = sum(
                    min(chunk_size, total_bytes - (i * chunk_size))
                    for i in self._completed_chunks
                )
            else:
                self._completed_chunks = set()
                self._completed_bytes = 0

            self._start_completed_bytes = self._completed_bytes
            self._start_time = time.monotonic()
            self._history.clear()
            self._history.append((self._start_time, self._completed_bytes))
            self._last_render_time = self._start_time
            was_started = self._started
            self._started = True
            self._has_started = True
            self._mode = mode

            if total_bytes == 0 and not was_started:
                self._spinner_thread = threading.Thread(target=self._spinner_loop, daemon=True)
                self._spinner_thread.start()
        self._render(force=True)

    def _spinner_loop(self) -> None:
        """Background loop to periodically redraw the progress bar when file size is unknown."""
        while True:
            with self._lock:
                if not self._started:
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
            if not self._started:
                return
            self._completed_bytes += bytes_downloaded
            if chunk_index is not None:
                self._completed_chunks.add(chunk_index)
            self._history.append((time.monotonic(), self._completed_bytes))
        self._render(force=False)

    def update_hashed(self, chunk_index: int) -> None:
        """Updates the progress bar that a chunk has been verified/hashed.

        Args:
            chunk_index: Index of the chunk that was just hashed.
        """
        with self._lock:
            if not self._started:
                return
            self._hashed_chunks.add(chunk_index)
        self._render(force=False)

    def close(self) -> None:
        """Closes the progress bar with a final newline (if standalone)."""
        was_started = False
        with self._lock:
            if self._started:
                was_started = True
                self._started = False
        if was_started:
            self._render(force=True)
            if self._render_callback is None:
                sys.stderr.write("\n")
                sys.stderr.flush()
            if self._spinner_thread is not None:
                self._spinner_thread.join(timeout=1.0)
                self._spinner_thread = None

    def set_overlay(self, text: str, success: bool = True, color: str | None = None) -> None:
        """Sets the state text to overlay on the progress bar.

        Args:
            text: The text to overlay (e.g., ' HASH OK ').
            success: Whether the state represents success (True) or failure (False).
            color: Optional color override ('red', 'yellow', 'green', 'blue').
        """
        with self._lock:
            self._overlay_text = text
            self._overlay_success = success
            self._overlay_color = color
        self._render(force=True)

    def set_throttled(self, is_throttled: bool) -> None:
        """Sets whether the download is currently throttled."""
        with self._lock:
            self._is_throttled = is_throttled
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
            started = self._started
        if started:
            self._render(force=True)

    def render_line(self, term_width: int, filename_width: int | None = None) -> str:
        """Renders the progress bar line contents for a given terminal width.

        Layout: Downloading <file> |<chunk_bar>| <pct>% <completed>/<total> <speed> ETA <eta>
        """
        with self._lock:
            total = self._total_bytes
            is_verify = self._mode == "verify"
            
            if is_verify:
                completed = min(len(self._hashed_chunks) * self._chunk_size, total)
            else:
                completed = min(self._completed_bytes, total)

            if total > 0:
                pct = (completed / total * 100)
            else:
                pct = 0.0

            now = time.monotonic()
            elapsed = now - self._start_time if self._start_time else 0.0

            if is_verify:
                speed = completed / elapsed if elapsed > 0.5 else 0.0
            else:
                # Calculate rolling speed over a 3-second window
                cutoff = now - 3.0
                while len(self._history) > 1 and self._history[1][0] < cutoff:
                    self._history.popleft()

                if len(self._history) > 0:
                    first_time, first_bytes = self._history[0]
                    elapsed_window = now - first_time
                    if elapsed_window > 0.5:
                        speed = (self._completed_bytes - first_bytes) / elapsed_window
                    else:
                        session_bytes = self._completed_bytes - self._start_completed_bytes
                        speed = session_bytes / elapsed if elapsed > 0.5 else 0.0
                else:
                    speed = 0.0

            if total == 0:
                # Formulate the spinner line
                SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
                spinner_char = SPINNER_FRAMES[int(now * 10) % len(SPINNER_FRAMES)]
                
                c_val, c_unit = _get_unit_and_value(completed)
                size_str = f"{c_val:.2f} {c_unit}"
                
                speed_val, speed_unit = _get_unit_and_value(speed)
                visible_speed_str = f"{speed_val:.2f} {speed_unit}/s"
                if self._is_throttled:
                    speed_str = f"\033[31m{visible_speed_str}\033[0m"
                else:
                    speed_str = visible_speed_str
                
                stats_visible = f" | {size_str} @ {visible_speed_str}"
                stats_formatted = f" | {size_str} @ {speed_str}"
                
                if self._overlay_text:
                    if self._overlay_color == "red":
                        bg_color = "\033[41;30m"
                    elif self._overlay_color == "yellow":
                        bg_color = "\033[43;30m"
                    elif self._overlay_color == "blue":
                        bg_color = "\033[44;30m"
                    elif self._overlay_color == "green":
                        bg_color = "\033[42;30m"
                    else:
                        if not self._overlay_success:
                            bg_color = "\033[41;30m"
                        elif "HASH OK" in self._overlay_text:
                            bg_color = "\033[44;30m"
                        else:
                            bg_color = "\033[42;30m"
                    status_suffix = f" {bg_color}{self._overlay_text}{RESET}"
                    status_len = len(self._overlay_text) + 1
                else:
                    status_suffix = f" {spinner_char} downloading (progress unavailable)"
                    status_len = len(status_suffix)
                
                needed_space = status_len + len(stats_visible)
                
                if term_width <= len(stats_visible):
                    return stats_visible[:term_width]
                
                filename_space = term_width - needed_space
                if filename_space >= 10:
                    display_filename = _truncate_filename(self._filename, filename_space)
                    return f"{display_filename}{status_suffix}{stats_formatted}"
                elif term_width >= needed_space + 4:
                    display_filename = _truncate_filename(self._filename, term_width - needed_space)
                    return f"{display_filename}{status_suffix}{stats_formatted}"
                else:
                    if self._overlay_text:
                        display_filename = _truncate_filename(self._filename, max(0, term_width - status_len))
                        return f"{display_filename}{status_suffix}"
                    else:
                        simple_status = f" {spinner_char} downloading"
                        simple_needed = len(simple_status) + len(stats_visible)
                        filename_space = term_width - simple_needed
                        if filename_space >= 5:
                            display_filename = _truncate_filename(self._filename, filename_space)
                            return f"{display_filename}{simple_status}{stats_formatted}"
                        else:
                            simple_status = f" {spinner_char}"
                            filename_space = term_width - len(simple_status) - len(stats_visible)
                            if filename_space > 0:
                                display_filename = _truncate_filename(self._filename, filename_space)
                                return f"{display_filename}{simple_status}{stats_formatted}"
                            else:
                                return stats_visible[:term_width]

            eta = (total - completed) / speed if (speed > 0 and total > 0) else 0.0

            # 1. Format stats string
            speed_val, speed_unit = _get_unit_and_value(speed)
            visible_speed_str = f" {speed_val:7.2f} {speed_unit:>3}/s"
            if self._is_throttled:
                speed_str = f" \033[31m{visible_speed_str.strip()}\033[0m"
            else:
                speed_str = visible_speed_str

            if self._mode == "verify":
                stats = f" {pct:5.1f}% ETA {_format_time(eta)}"
                stats_len = len(stats)
            elif total > 0:
                t_val, t_unit = _get_unit_and_value(total)
                c_val = completed
                for u in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
                    if u == t_unit:
                        break
                    c_val /= 1024.0
                t_width = len(f"{t_val:.2f}")
                size_str = f"{c_val:{t_width}.2f}/{t_val:{t_width}.2f} {t_unit:>3}"
                stats = (
                    f"{pct:5.1f}%"
                    f" {size_str}"
                    f"{speed_str}"
                    f" ETA {_format_time(eta)}"
                )
                stats_len = len(f"{pct:5.1f}% {size_str}{visible_speed_str} ETA {_format_time(eta)}")
            else:
                c_val, c_unit = _get_unit_and_value(completed)
                size_str = f"{c_val:7.2f} {c_unit:>3}"
                stats = (
                    f" {size_str}"
                    f"{speed_str}"
                )
                stats_len = len(f" {size_str}{visible_speed_str}")

            # 2. Check if we have enough space for stats
            if term_width <= stats_len:
                return stats[:term_width]

            # 3. Space left for label prefix and filename
            label_prefix = ""
            rem_width = term_width - stats_len

            # If rem_width cannot fit the label_prefix, truncate the prefix + filename
            if rem_width <= len(label_prefix):
                return (label_prefix + self._filename)[:rem_width]

            # Space available for filename and bar combined
            space_for_file_and_bar = rem_width - len(label_prefix)

            # A progress bar needs " |" (2 chars) and "| " (2 chars) plus at least 10 chars for bar itself.
            # So minimum needed is 14 characters.
            # The filename should ideally get at least 10 characters (or its full length if shorter).
            min_bar_len = 10
            min_filename_len = 10
            needed_filename_len = min(len(self._filename), min_filename_len)
            min_needed_space = needed_filename_len + 4 + min_bar_len

            if space_for_file_and_bar >= min_needed_space and total > 0 and self._total_chunks > 1:
                # We can show a progress bar!
                # Allocate filename length. When filename_width is provided by MultiProgress,
                # use it as the target column width so all bars align. Otherwise fall back to
                # the actual filename length so standalone usage is unaffected.
                effective_name_len = filename_width if filename_width is not None else len(self._filename)
                filename_len = min(effective_name_len, space_for_file_and_bar - 4 - min_bar_len)

                # Now calculate the exact bar width so the line fits term_width exactly
                bar_width = space_for_file_and_bar - filename_len - 4

                # Truncate filename if it is longer than allocated filename_len
                display_filename = self._filename
                if len(display_filename) > filename_len:
                    display_filename = _truncate_filename(display_filename, filename_len)
                    # Re-verify filename_len in case of rounding, adjust bar_width
                    filename_len = len(display_filename)
                    bar_width = space_for_file_and_bar - filename_len - 4

                # Pad shorter filenames so the | delimiter lines up across all bars
                display_filename = display_filename.ljust(filename_len)

                label = f"{label_prefix}{display_filename}"
                bar = self._build_bar(bar_width)
                line = f"{label} |{bar}{RESET}| {stats}"
            else:
                # No progress bar. Just show truncated filename + stats.
                filename_len = space_for_file_and_bar
                display_filename = self._filename
                if len(display_filename) > filename_len:
                    display_filename = _truncate_filename(display_filename, filename_len)
                line = f"{label_prefix}{display_filename}{stats}"

            return line


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

    def _build_bar(self, width: int) -> str:
        """Builds the chunk-mapped progress bar string.

        Maps each screen character to a range of chunks and uses Unicode
        shading levels to represent partial completion within that region.

        Args:
            width: The number of characters available for the bar.

        Returns:
            The formatted bar string with ANSI color codes.
        """
        if self._total_chunks == 0 or width == 0:
            return GRAY + (BLOCK_LOW * width)

        completed_arr = [0.0] * width
        hashed_arr = [0.0] * width

        for col in range(width):
            start_chunk_frac = col * self._total_chunks / width
            end_chunk_frac = (col + 1) * self._total_chunks / width
            
            start_chunk = int(start_chunk_frac)
            end_chunk = math.ceil(end_chunk_frac)
            
            if end_chunk - start_chunk == 1:
                chunk_idx = start_chunk
                if chunk_idx in self._hashed_chunks:
                    hashed_arr[col] = 1.0
                if chunk_idx in self._completed_chunks:
                    completed_arr[col] = 1.0
            else:
                total_in_col = 0
                completed_in_col = 0
                hashed_in_col = 0
                for c in range(start_chunk, end_chunk):
                    total_in_col += 1
                    if c in self._completed_chunks:
                        completed_in_col += 1
                    if c in self._hashed_chunks:
                        hashed_in_col += 1
                if total_in_col > 0:
                    completed_arr[col] = completed_in_col / total_in_col
                    hashed_arr[col] = hashed_in_col / total_in_col

        chars: list[str] = []
        for i in range(width):
            hashed_ratio = hashed_arr[i]
            completed_ratio = completed_arr[i]

            if hashed_ratio > 0:
                if hashed_ratio <= 0.5:
                    chars.append(GREEN + BLOCK_MED)
                elif hashed_ratio < 1.0:
                    chars.append(GREEN + BLOCK_HIGH)
                else:
                    chars.append(GREEN + BLOCK_FULL)
            elif completed_ratio > 0:
                if completed_ratio <= 0.5:
                    chars.append(BLUE + BLOCK_MED)
                elif completed_ratio < 1.0:
                    chars.append(BLUE + BLOCK_HIGH)
                else:
                    chars.append(BLUE + BLOCK_FULL)
            else:
                chars.append(GRAY + BLOCK_LOW)

        if self._overlay_text and len(self._overlay_text) <= width:
            text_len = len(self._overlay_text)
            start_idx = (width - text_len) // 2
            
            if self._overlay_color == "red":
                bg_color = "\033[41;30m"
            elif self._overlay_color == "yellow":
                bg_color = "\033[43;30m"
            elif self._overlay_color == "blue":
                bg_color = "\033[44;30m"
            elif self._overlay_color == "green":
                bg_color = "\033[42;30m"
            else:
                if not self._overlay_success:
                    bg_color = "\033[41;30m"  # Red bg, black text
                elif "HASH OK" in self._overlay_text:
                    bg_color = "\033[44;30m"  # Blue bg, black text
                else:
                    bg_color = "\033[42;30m"  # Green bg, black text
                    
            for i, char in enumerate(self._overlay_text):
                chars[start_idx + i] = f"{bg_color}{char}{RESET}"

        return "".join(chars)


class MultiProgress:
    """Thread-safe coordinator for rendering multiple progress bars stacked on top of each other."""

    def __init__(self, refresh_interval: float = 0.2) -> None:
        self._lock = threading.Lock()
        self._bars: list[BuiltinProgress] = []
        self._last_render_time = 0.0
        self._refresh_interval = refresh_interval
        self._lines_printed = 0
        self._is_closed = False

    def add_bar(self) -> BuiltinProgress:
        """Adds and returns a new progress bar managed by this coordinator."""
        with self._lock:
            bar = BuiltinProgress(
                render_callback=self._child_update_callback,
                log_callback=self.log
            )
            self._bars.append(bar)
            return bar

    def _child_update_callback(self, child: BuiltinProgress, force: bool = False) -> None:
        """Called by a child progress bar when it wants to trigger a render."""
        self._render(force=force)

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
                (len(bar._filename) for bar in self._bars if bar._has_started),
                default=0,
            )

            lines = []
            for bar in self._bars:
                if bar._has_started:
                    lines.append(bar.render_line(term_width, filename_width=max_filename_len))

            if not lines:
                return

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
                sys.stderr.write("\n".join(lines) + "\n")
                sys.stderr.flush()

    def close(self) -> None:
        """Closes the progress coordinator and prints the final states."""
        with self._lock:
            if self._is_closed:
                return
            self._is_closed = True

            term_width = _get_term_width()

            max_filename_len = max(
                (len(bar._filename) for bar in self._bars),
                default=0,
            )

            lines = []
            for bar in self._bars:
                lines.append(bar.render_line(term_width, filename_width=max_filename_len))

            if lines:
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
                else:
                    sys.stderr.write("\n".join(lines) + "\n")
                sys.stderr.flush()

            self._lines_printed = 0

    def log(self, message: str) -> None:
        """Logs a message safely by printing it above the stacked progress bars."""
        with self._lock:
            if self._is_closed:
                sys.stdout.write(f"{message}\n")
                sys.stdout.flush()
                return

            term_width = _get_term_width()

            max_filename_len = max(
                (len(bar._filename) for bar in self._bars if bar._has_started),
                default=0,
            )

            lines = []
            for bar in self._bars:
                if bar._has_started:
                    lines.append(bar.render_line(term_width, filename_width=max_filename_len))

            is_tty = sys.stderr.isatty()

            if is_tty:
                if self._lines_printed > 0:
                    # Move up and clear each line on stderr
                    sys.stderr.write(f"\r\033[{self._lines_printed}A")
                    for _ in range(self._lines_printed):
                        sys.stderr.write("\033[K\n")
                    # Move back up to the start
                    sys.stderr.write(f"\r\033[{self._lines_printed}A")
                    sys.stderr.flush()
                
                msg_lines = message.split("\n")
                for msg_line in msg_lines:
                    sys.stdout.write(f"{msg_line}\n")
                sys.stdout.flush()

                for line in lines:
                    sys.stderr.write(f"\r{line}\033[K\n")
                
                if len(lines) < self._lines_printed:
                    for _ in range(self._lines_printed - len(lines)):
                        sys.stderr.write("\r\033[K\n")
                    sys.stderr.write(f"\033[{self._lines_printed - len(lines)}A")
                
                sys.stderr.flush()
                self._lines_printed = len(lines)
            else:
                sys.stdout.write(f"{message}\n")
                sys.stdout.flush()
                if lines:
                    sys.stderr.write("\n".join(lines) + "\n")
                    sys.stderr.flush()


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
