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

    print("Setting up download for pause and resume demonstration...")

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
        # Pause after 10 seconds
        await asyncio.sleep(10)
        downloader.pause()

        # Wait a bit while paused
        await asyncio.sleep(5)
        
        # Resume download
        downloader.resume()
        
        # Wait for the download to finish
        await task

    except asyncio.CancelledError:
        downloader.cancel()
        await task
        
if __name__ == "__main__":
    asyncio.run(main())
