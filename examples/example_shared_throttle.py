import asyncio
from mrdl import Downloader, MultiProgress, DownloadConfig
from mrdl.throttle import TokenBucketThrottle

async def download_file(filename, paths, expected_hash, progress_bar, throttle):
    try:
        config = DownloadConfig(
            urls=paths,
            filename=filename,
            threads_per_mirror=2,
            chunk_size=1024 * 1024,
            min_speed_kbps=100,
            speed_grace_period=10,
            checksum=expected_hash,
        )
        downloader = Downloader(config, progress=progress_bar, global_throttle=throttle)

        result = await downloader.start()
        if result.status.name != "COMPLETED":
            print(f"Download failed for {filename}")
    except asyncio.CancelledError:
        pass

async def main():
    mirrors = [
        "https://cofractal-ewr.mm.fcix.net/ubuntu-releases/",
        "https://ohioix.mm.fcix.net/ubuntu-releases/",
        "https://gigsouth.mm.fcix.net/ubuntu-releases/"
    ]

    file1 = "ubuntu-26.04-desktop-amd64.iso"
    paths1 = [item + f"26.04/{file1}" for item in mirrors]
    expected_hash1 = "sha256:487f87faaf547ea30e0aba4d5b53346292571256b25333a978db1692bcee9dd2"

    file2 = "ubuntu-26.04-live-server-amd64.iso"
    paths2 = [item + f"26.04/{file2}" for item in mirrors]
    expected_hash2 = "sha256:dec49008a71f6098d0bcfc822021f4d042d5f2db279e4d75bdd981304f1ca5d9"

    print("Starting downloads with a shared 5 MB/s global throttle...")

    progress_manager = MultiProgress()
    bar1 = progress_manager.add_bar()
    bar2 = progress_manager.add_bar()

    # Create a single 5MB/s throttle to share across both downloads
    shared_throttle = TokenBucketThrottle(rate_kbps=5000)

    task1 = asyncio.create_task(download_file(file1, paths1, expected_hash1, bar1, shared_throttle))
    task2 = asyncio.create_task(download_file(file2, paths2, expected_hash2, bar2, shared_throttle))

    try:
        await asyncio.gather(task1, task2)
    except asyncio.CancelledError:
        task1.cancel()
        task2.cancel()
        await asyncio.gather(task1, task2)
    finally:
        progress_manager.close()

if __name__ == "__main__":
    asyncio.run(main())
