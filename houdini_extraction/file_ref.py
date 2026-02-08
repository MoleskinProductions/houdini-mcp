"""
File reference management for large data extraction.

Manages the shared temp directory /tmp/pixel_vision/extract/ and provides
helpers for writing file references (§3.3) and garbage collecting expired files.
"""

from __future__ import annotations

import os
import time
import uuid

EXTRACT_DIR = '/tmp/pixel_vision/extract'
DEFAULT_TTL = 300  # seconds


def ensure_extract_dir() -> str:
    """Create the extraction temp directory if it doesn't exist.

    Returns:
        The absolute path to the extraction directory.
    """
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    return EXTRACT_DIR


def write_file_ref(
    data: bytes,
    extension: str,
    *,
    prefix: str = 'extract',
    ttl_seconds: int = DEFAULT_TTL,
) -> dict[str, object]:
    """Write binary data to the extract dir and return a file_ref dict.

    Args:
        data: Raw bytes to write.
        extension: File extension including dot (e.g., '.bin', '.bgeo.sc').
        prefix: Filename prefix for identification.
        ttl_seconds: Time-to-live before GC may clean up the file.

    Returns:
        A file_ref dict matching §3.3 schema.
    """
    ensure_extract_dir()
    unique = uuid.uuid4().hex[:12]
    filename = f'{prefix}_{unique}{extension}'
    filepath = os.path.join(EXTRACT_DIR, filename)

    with open(filepath, 'wb') as f:
        f.write(data)

    return {
        'type': 'file_ref',
        'path': filepath,
        'format': extension.lstrip('.'),
        'size_bytes': len(data),
        'ttl_seconds': ttl_seconds,
    }


def write_file_ref_pair(
    binary_data: bytes,
    metadata: dict[str, object],
    binary_ext: str,
    *,
    prefix: str = 'attrib',
    ttl_seconds: int = DEFAULT_TTL,
) -> dict[str, object]:
    """Write binary data + JSON metadata sidecar and return a file_ref.

    Used for large attribute reads where we write flat float32 binary
    with a JSON sidecar containing shape/type metadata.

    Args:
        binary_data: Raw binary data (e.g., struct.pack'd float32).
        metadata: Metadata dict to write as JSON sidecar.
        binary_ext: Extension for the binary file (e.g., '.bin').
        prefix: Filename prefix.
        ttl_seconds: TTL for both files.

    Returns:
        A file_ref dict with an additional 'metadata_path' field.
    """
    import json

    ensure_extract_dir()
    unique = uuid.uuid4().hex[:12]
    bin_filename = f'{prefix}_{unique}{binary_ext}'
    meta_filename = f'{prefix}_{unique}.json'
    bin_path = os.path.join(EXTRACT_DIR, bin_filename)
    meta_path = os.path.join(EXTRACT_DIR, meta_filename)

    with open(bin_path, 'wb') as f:
        f.write(binary_data)

    with open(meta_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    return {
        'type': 'file_ref',
        'path': bin_path,
        'metadata_path': meta_path,
        'format': binary_ext.lstrip('.'),
        'size_bytes': len(binary_data),
        'ttl_seconds': ttl_seconds,
    }


def gc_expired_files(max_age_seconds: int = DEFAULT_TTL) -> int:
    """Remove files older than max_age_seconds from the extract directory.

    Args:
        max_age_seconds: Delete files older than this many seconds.

    Returns:
        Number of files deleted.
    """
    if not os.path.isdir(EXTRACT_DIR):
        return 0

    now = time.time()
    deleted = 0

    for filename in os.listdir(EXTRACT_DIR):
        filepath = os.path.join(EXTRACT_DIR, filename)
        if not os.path.isfile(filepath):
            continue
        try:
            mtime = os.path.getmtime(filepath)
            if now - mtime > max_age_seconds:
                os.remove(filepath)
                deleted += 1
        except OSError:
            continue

    return deleted
