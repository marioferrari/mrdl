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

    print(f"Starting download from {len(mirrors)} mirrors...")

    try:
        config = DownloadConfig(
            urls=paths,
            filename=filename,
            threads_per_mirror=8,
            chunk_size=1024 * 1024,
            min_speed_kbps=1024,
            speed_grace_period=10,
            checksum=expected_hash
        )
        downloader = Downloader(config)

        result = await downloader.start()
        
        print(f"Download took {result.time_taken:.2f} seconds.")
        if result.computed_hash:
            print(f"Computed hash ({config.checksum}): {result.computed_hash}")

        if result.status.name != "COMPLETED":
            print(f"Download failed with status: {result.status.value}")
            if result.error:
                print(f"Error details: {result.error}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
