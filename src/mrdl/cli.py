import argparse
import asyncio
import sys
import uvloop
from mrdl.downloader import Downloader
from mrdl.types import DownloadConfig, DownloadState


def parse_args(args=None):
    """Parses command-line arguments for the multi-mirror downloader.

    Args:
        args: Optional list of arguments to parse. Defaults to sys.argv[1:].

    Returns:
        The parsed Namespace containing command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="mrdl - A resilient multi-mirror concurrent downloader."
    )
    parser.add_argument(
        "urls",
        nargs="+",
        help="One or more mirror URLs pointing to the file to download.",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Local output filename/path to save the downloaded file.",
    )
    parser.add_argument(
        "-t", "--threads-per-mirror",
        type=int,
        default=1,
        help="Number of threads per mirror (default: 1).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=64 * 1024 * 1024,
        help="Chunk size in bytes (default: 67108864 / 64MB).",
    )
    parser.add_argument(
        "--min-speed",
        type=int,
        default=1024,
        help="Minimum download speed per mirror thread in KB/s. Slow mirrors will be throttled/banned (default: 1024).",
    )
    parser.add_argument(
        "--grace-period",
        type=int,
        default=10,
        help="Speed grace period in seconds before applying minimum speed check (default: 10).",
    )
    parser.add_argument(
        "--speed-ema",
        type=float,
        default=1.0,
        help="Time constant in seconds for smoothing the download speed metric (default: 1.0).",
    )
    parser.add_argument(
        "--speed-update-interval",
        type=float,
        default=0.2,
        help="Interval in seconds to update the download speed and ETA display (default: 0.2).",
    )
    parser.add_argument(
        "--checksum",
        dest="checksum",
        metavar="ALGO[:EXPECTED_HEX]",
        help=(
            "Hash algorithm for integrity verification. "
            "Use 'algo' to compute and print (e.g. sha256), "
            "or 'algo:expected_hex' to compute and verify (e.g. sha256:abc123...). "
            "Supports any algorithm available in Python's hashlib "
            "(md5, sha256, sha512, sha3_256, blake2b, ...)."
        ),
    )
    parser.add_argument(
        "--max-speed",
        type=int,
        default=None,
        metavar="KB/S",
        help="Global download speed cap in KB/s across all threads combined (default: uncapped).",
    )
    parser.add_argument(
        "--max-speed-per-thread",
        type=int,
        default=None,
        metavar="KB/S",
        help="Per-thread download speed cap in KB/s (default: uncapped).",
    )
    parser.add_argument(
        "-s", "--silent",
        action="store_true",
        help="Run in silent mode. Suppresses all progress output and warnings.",
    )
    parser.add_argument(
        "--use-mmap",
        action="store_true",
        help="Use memory-mapped file writing. Warning: Known to cause APFS corruption on macOS.",
    )
    return parser.parse_args(args)


def main():
    """Main execution entry point for the CLI.

    Initializes the downloader and manages process termination and errors.
    """
    args = parse_args()

    config = DownloadConfig(
        urls=args.urls,
        filename=args.output,
        threads_per_mirror=args.threads_per_mirror,
        chunk_size=args.chunk_size,
        min_speed_kbps=args.min_speed,
        speed_grace_period=args.grace_period,
        speed_ema_window=args.speed_ema,
        speed_update_interval=args.speed_update_interval,
        checksum=args.checksum,
        max_speed_kbps=args.max_speed,
        max_speed_per_thread_kbps=args.max_speed_per_thread,
        silent=args.silent,
        use_mmap=args.use_mmap,
    )
    
    downloader = Downloader(config)

    try:
        result = asyncio.run(downloader.start(), loop_factory=uvloop.new_event_loop)
        print(f"Time taken: {result.time_taken:.2f}s")
        if result.status == DownloadState.COMPLETED:
            if config.checksum and not result.hash_matched:
                print("Download completed but hash verification failed.")
                sys.exit(1)
            print("Download completed successfully.")
            sys.exit(0)
        else:
            print(f"Download failed with status: {result.status.value}")
            sys.exit(1)
    except KeyboardInterrupt:
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()
        sys.exit(130)
    except Exception as e:
        print(f"Error during download: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
