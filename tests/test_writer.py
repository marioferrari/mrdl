import os
import tempfile
import threading
import unittest

from mrdl.writer import DiskWriter


class TestDiskWriter(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpfile = tempfile.NamedTemporaryFile(delete=False)
        self.tmpfile.write(b"\x00" * 1024)
        self.tmpfile.close()
        self.fd = os.open(self.tmpfile.name, os.O_RDWR)
        self.stop_event = threading.Event()

    def tearDown(self):
        try:
            os.close(self.fd)
        except OSError:
            pass
        os.unlink(self.tmpfile.name)

    async def test_write_and_read_back(self):
        writer = DiskWriter(self.fd, self.stop_event)
        writer.start()

        await writer.write(0, b"hello")
        await writer.write(100, b"world")
        writer.stop()

        os.lseek(self.fd, 0, os.SEEK_SET)
        data = os.read(self.fd, 5)
        assert data == b"hello"

        os.lseek(self.fd, 100, os.SEEK_SET)
        data = os.read(self.fd, 5)
        assert data == b"world"

    async def test_mark_complete_tracks_chunks(self):
        writer = DiskWriter(self.fd, self.stop_event)
        writer.start()

        assert writer.is_on_disk(0) is False

        await writer.mark_complete(0)
        writer.stop()

        assert writer.is_on_disk(0) is True
        assert writer.is_on_disk(1) is False

    def test_stop_without_writes(self):
        writer = DiskWriter(self.fd, self.stop_event)
        writer.start()
        writer.stop()
        assert writer.error is None

    async def test_multiple_marks(self):
        writer = DiskWriter(self.fd, self.stop_event)
        writer.start()

        await writer.mark_complete(0)
        await writer.mark_complete(3)
        await writer.mark_complete(7)
        writer.stop()

        assert writer.is_on_disk(0) is True
        assert writer.is_on_disk(3) is True
        assert writer.is_on_disk(7) is True
        assert writer.is_on_disk(1) is False

    async def test_concurrent_writes(self):
        writer = DiskWriter(self.fd, self.stop_event)
        writer.start()
        
        import asyncio

        async def worker(start_offset: int, chunk_idx: int):
            data = str(chunk_idx).encode() * 10
            await writer.write(start_offset, data)
            await writer.mark_complete(chunk_idx)

        tasks = []
        for i in range(10):
            t = asyncio.create_task(worker(i * 10, i))
            tasks.append(t)

        await asyncio.gather(*tasks)

        writer.stop()

        for i in range(10):
            assert writer.is_on_disk(i) is True
            os.lseek(self.fd, i * 10, os.SEEK_SET)
            data = os.read(self.fd, 10)
            assert data == str(i).encode() * 10

    async def test_write_failure_sets_error_and_stop_event(self):
        writer = DiskWriter(self.fd, self.stop_event)
        writer.start()

        # Close the fd to simulate an I/O error on pwrite
        os.close(self.fd)

        await writer.write(0, b"hello")
        writer.stop()
        
        assert writer.error is not None
        assert isinstance(writer.error, OSError)
        assert self.stop_event.is_set() is True

