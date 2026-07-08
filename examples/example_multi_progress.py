import sys
import asyncio
from mrdl import Downloader, MultiProgress, DownloadConfig

async def download_file(downloader, filename):
    try:
        result = await downloader.start()
        if result.status.name != "COMPLETED":
            print(f"Download failed for {filename} with status {result.status.value}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"An unexpected error occurred for {filename}: {e}")

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

    print("Starting multiple concurrent downloads with stacked progress bars...")

    progress_manager = MultiProgress()
    bar1 = progress_manager.add_bar()
    bar2 = progress_manager.add_bar()

    config1 = DownloadConfig(
        urls=paths1,
        filename=file1,
        threads_per_mirror=2,
        chunk_size=1024 * 1024,
        min_speed_kbps=100,
        speed_grace_period=10,
        checksum=expected_hash1,
    )
    downloader1 = Downloader(config1, progress=bar1)
    
    config2 = DownloadConfig(
        urls=paths2,
        filename=file2,
        threads_per_mirror=4,
        chunk_size=1024 * 1024,
        min_speed_kbps=100,
        speed_grace_period=10,
        checksum=expected_hash2,
    )
    downloader2 = Downloader(config2, progress=bar2)

    task1 = asyncio.create_task(download_file(downloader1, file1))
    task2 = asyncio.create_task(download_file(downloader2, file2))

    try:
        await asyncio.gather(task1, task2)
    except asyncio.CancelledError:
        downloader1.stop()
        downloader2.stop()
        await asyncio.gather(task1, task2)
    finally:
        progress_manager.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()
