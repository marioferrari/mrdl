import pytest
from mrdl.prober import MirrorProber
from mrdl.types import FileMetadata
from werkzeug.wrappers import Response


@pytest.mark.asyncio
async def test_successful_probe_returns_metadata(httpserver):
    prober = MirrorProber()
    httpserver.expect_request("/file.bin").respond_with_response(
        Response(status=206, headers={
            "Content-Range": "bytes 0-0/1048576",
            "ETag": '"abc123"',
            "Last-Modified": "Tue, 01 Jan 2025 00:00:00 GMT",
        })
    )

    result = await prober.probe([httpserver.url_for("/file.bin")])

    assert result.total_size == 1048576
    assert result.accepts_ranges is True
    assert result.etag == '"abc123"'
    assert result.last_modified == "Tue, 01 Jan 2025 00:00:00 GMT"


@pytest.mark.asyncio
async def test_non_range_response_skips_mirror(httpserver):
    prober = MirrorProber()
    httpserver.expect_request("/file.bin").respond_with_response(
        Response(status=200, headers={})
    )

    result = await prober.probe([httpserver.url_for("/file.bin")])

    assert result.total_size == 0
    assert result.accepts_ranges is False


@pytest.mark.asyncio
async def test_connection_error_falls_through():
    prober = MirrorProber()
    # No server is listening on this fake address
    with pytest.raises(FileNotFoundError):
        await prober.probe(["http://localhost:1/file.bin"])


@pytest.mark.asyncio
async def test_first_mirror_fails_second_succeeds(httpserver):
    prober = MirrorProber()
    httpserver.expect_request("/good.bin").respond_with_response(
        Response(status=206, headers={
            "Content-Range": "bytes 0-0/500",
        })
    )

    result = await prober.probe([
        "http://localhost:1/bad.bin",  # Will fail immediately
        httpserver.url_for("/good.bin"),
    ])

    assert result.total_size == 500
    assert result.accepts_ranges is True


@pytest.mark.asyncio
async def test_missing_content_range_slash_returns_none(httpserver):
    prober = MirrorProber()
    httpserver.expect_request("/file.bin").respond_with_response(
        Response(status=206, headers={"Content-Range": "bytes 0-0"})
    )

    with pytest.raises(FileNotFoundError):
        await prober.probe([httpserver.url_for("/file.bin")])


@pytest.mark.asyncio
async def test_malformed_headers_handled_gracefully(httpserver):
    prober = MirrorProber()
    httpserver.expect_request("/file.bin").respond_with_response(
        Response(status=206, headers={"Content-Range": "bytes 0-0/abc"})
    )

    with pytest.raises(FileNotFoundError):
        await prober.probe([httpserver.url_for("/file.bin")])
