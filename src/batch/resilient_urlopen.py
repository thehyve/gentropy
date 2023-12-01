"""Resilient retrieval of HTTP(S) and FTP data with block-level retries.

The "resilient_urlopen" class, which is contained in this module, provides an interface similar to urllib.request.urlopen and allows a URL to be read as a bytes stream. However, in contrast to the urllib implementation, if a transient error occurs partway through the input file, then only the failed part is retried.

This module currently supports HTTP(S) and FTP protocols. In order for it to work, the server must support resumable downloads, which is true for most modern HTTP(S) and FTP servers.

Usage:
    ```
    with resilient_urlopen("http://example.com") as bytes_stream:
        # Any usual bytes stream processing.
        # Retries are handled seamlessly by the class under the hood.
    ```
"""

from __future__ import annotations

import ftplib
import os
import random
import time
import urllib.parse
import urllib.request
from typing import Any

import ftputil


class resilient_urlopen:
    """Resilient HTTP and FTP data retrieval."""

    # Delay values in seconds.
    delay_initial = 1.0
    delay_increase_factor = 1.5
    delay_max = 120.0
    delay_jitter = 3.0
    delay_give_up = 3600.0

    # Fetch value in bytes.
    fetch_block_size = 200 * 1024 * 1024

    def __init__(self, uri: str):
        """Initialise the class.

        Args:
            uri (str): The URI to read the data from.

        Raises:
            NotImplementedError: If the protocol is not HTTP(S) or FTP.
        """
        self.uri = uri
        self.buffer = b""
        self.buffer_position = 0
        self.url_position = 0
        while True:
            try:
                if uri.startswith("http"):
                    self.content_length = int(
                        urllib.request.urlopen(uri).getheader("Content-Length")
                    )
                elif uri.startswith("ftp"):
                    parsed_uri = urllib.parse.urlparse(uri)
                    self.ftp_server = parsed_uri.netloc
                    self.ftp_path, self.ftp_filename = os.path.split(
                        parsed_uri.path[1:]
                    )
                    with ftplib.FTP(self.ftp_server) as ftp:
                        ftp.login()
                        ftp.cwd(self.ftp_path)
                        length = ftp.size(self.ftp_filename)
                        assert (
                            length is not None
                        ), f"FTP server returned no length for {uri}."
                        self.content_length = length
                else:
                    raise NotImplementedError(f"Unsupported URI schema: {uri}.")
                break
            except Exception:
                time.sleep(5 + random.random())
        assert self.content_length > 0

    def __enter__(self) -> "resilient_urlopen":
        """Stream reading entry point.

        Returns:
            resilient_urlopen: An instance of the class
        """
        return self

    def __exit__(self, *args: Any) -> None:
        """Stream reading exit point (empty).

        Args:
            *args (Any): ignored.
        """
        pass

    def read(self, size: int) -> bytes:
        """Stream reading method.

        Args:
            size(int): How many bytes to read.

        Returns:
            bytes: A block of data from the requested position and length.

        Raises:
            Exception: If a block could not be read from the URI exceeding the maximum delay time.
            NotImplementedError: If the protocol is not HTTP(s) or FTP.
        """
        # Trim spent part of the buffer, if necessary.
        if self.buffer_position > self.fetch_block_size:
            self.buffer_position -= self.fetch_block_size
            self.buffer = self.buffer[self.fetch_block_size :]

        # If the buffer isn't enough to serve next block, we need to extend it first.
        while (size > len(self.buffer) - self.buffer_position) and (
            self.url_position != self.content_length
        ):
            delay = self.delay_initial
            total_delay = 0.0
            while True:
                try:
                    if self.uri.startswith("http"):
                        byte_range = f"bytes={self.url_position}-{self.url_position + self.fetch_block_size - 1}"
                        request = urllib.request.Request(
                            self.uri, headers={"Range": byte_range}
                        )
                        block = urllib.request.urlopen(request).read()
                    elif self.uri.startswith("ftp"):
                        with ftputil.FTPHost(
                            self.ftp_server, "anonymous", "anonymous"
                        ) as ftp_host:
                            with ftp_host.open(
                                f"{self.ftp_path}/{self.ftp_filename}",
                                mode="rb",
                                rest=self.url_position,
                            ) as stream:
                                block = stream.read(self.fetch_block_size)
                    else:
                        raise NotImplementedError(
                            f"Unsupported URI schema: {self.uri}."
                        )
                    self.buffer += block
                    self.url_position += len(block)
                    break
                except Exception as e:
                    total_delay += delay
                    if total_delay > self.delay_give_up:
                        raise Exception(
                            f"Could not fetch URI {self.uri} at position {self.url_position}, length {size} after {total_delay} seconds"
                        ) from e
                    time.sleep(delay)
                    delay = (
                        min(delay * self.delay_increase_factor, self.delay_max)
                        + self.delay_jitter * random.random()
                    )

        # Return next block from the buffer.
        data = self.buffer[self.buffer_position : self.buffer_position + size]
        self.buffer_position += size
        return data
