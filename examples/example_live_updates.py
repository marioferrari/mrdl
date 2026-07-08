import sys
import asyncio
from mrdl import Downloader, DownloadConfig

async def main():
    mirrors = [
        "https://cofractal-ewr.mm.fcix.net/ubuntu-releases/",
        "https://ohioix.mm.fcix.net/ubuntu-releases/",
        "https://gigsouth.mm.fcix.net/ubuntu-releases/"
    ]

    filename = "ubuntu-26.04-live-server-amd64.iso"
    paths = [item + f"26.04/{filename}" for item in mirrors]
    expected_hash = "sha256:dec49008a71f6098d0bcfc822021f4d042d5f2db279e4d75bdd981304f1ca5d9"

    print("Starting download with dynamic speed limit updates...")

    config = DownloadConfig(
        urls=paths, 
        filename=filename,
        threads_per_mirror=2,
        checksum=expected_hash
    )
    downloader = Downloader(config)

    # Run download in a background task
    task = asyncio.create_task(downloader.start())

    try:
        await asyncio.sleep(5)
        downloader.set_speed_limit(1024)

        await asyncio.sleep(10)
        downloader.set_speed_limit(5120)

        await asyncio.sleep(10)
        downloader.set_speed_limit(None)

        await task

    except asyncio.CancelledError:
        downloader.stop()
        await task
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()