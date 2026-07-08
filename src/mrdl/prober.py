from __future__ import annotations

import asyncio
import aiohttp

from mrdl.types import FileMetadata

class MirrorProber:
    """Probes mirror endpoints to gather file metadata and capability information."""

    async def probe(self, urls: list[str], timeout: int = 5) -> FileMetadata:
        """Probes mirror URLs concurrently to find the first one that responds with valid metadata.

        Args:
            urls: List of mirror URLs to probe.
            timeout: Network timeout in seconds.

        Returns:
            The retrieved FileMetadata, or a default empty metadata if all probes fail.
        """
        if not urls:
            raise ValueError("No URLs provided to probe.")

        fallback_result = None
        
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            tasks = [asyncio.create_task(self._probe_single(session, url)) for url in urls]
            
            for task in asyncio.as_completed(tasks):
                result = await task
                if result is not None:
                    if result.accepts_ranges:
                        # Cancel remaining tasks
                        for t in tasks:
                            if not t.done():
                                t.cancel()
                        # Return the first successful range-supporting result immediately.
                        return result
                    elif fallback_result is None:
                        fallback_result = result
            
            if fallback_result is not None:
                return fallback_result

        raise FileNotFoundError("File not found on any of the provided mirrors.")

    async def _probe_single(self, session: aiohttp.ClientSession, url: str) -> FileMetadata | None:
        """Probes a single mirror URL to check availability and range request support.

        Args:
            session: The aiohttp ClientSession to use.
            url: The mirror URL to probe.

        Returns:
            FileMetadata if the probe is successful, otherwise None.
        """
        try:
            headers = {"Range": "bytes=0-0"}
            async with session.get(url, headers=headers) as response:
                if response.status == 206:
                    return self._parse_range_response(response)
                elif response.status == 200:
                    total_size = int(response.headers.get("Content-Length", 0))
                    return FileMetadata(
                        total_size=total_size,
                        accepts_ranges=False,
                        etag=response.headers.get("ETag"),
                        last_modified=response.headers.get("Last-Modified"),
                    )
        except (aiohttp.ClientError, ValueError, asyncio.TimeoutError):
            pass

        return None

    def _parse_range_response(self, response: aiohttp.ClientResponse) -> FileMetadata | None:
        """Parses the HTTP range response headers to build FileMetadata.

        Args:
            response: The aiohttp ClientResponse object from the probe.

        Returns:
            FileMetadata if headers are valid, otherwise None.
        """
        content_range = response.headers.get("Content-Range", "")
        if "/" not in content_range:
            return None

        total_size = int(content_range.split("/")[-1])
        return FileMetadata(
            total_size=total_size,
            accepts_ranges=True,
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
        )
