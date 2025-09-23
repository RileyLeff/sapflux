#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///

import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import requests


@dataclass
class ResponseData:
    status: str
    json: dict
    raw_text: str


def run(cmd: List[str], *, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, **kwargs)


def wait_for_api(url: str, timeout: int = 180, interval: int = 2) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            if resp.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(interval)
    raise RuntimeError(f"API at {url} did not become healthy within {timeout}s")


class LazyFile:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh = None

    def read(self, size: int = -1) -> bytes:
        if self._fh is None:
            self._fh = open(self.path, "rb")
        data = self._fh.read(size)
        if not data:
            self.close()
        return data

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __del__(self) -> None:  # best-effort cleanup
        try:
            self.close()
        except Exception:
            pass


def build_multipart(manifest: Path, raw_files: Iterable[Path]) -> List[Tuple[str, Tuple[str, object, str]]]:
    files: List[Tuple[str, Tuple[str, object, str]]] = []
    files.append(
        (
            "metadata_manifest",
            (manifest.name, open(manifest, "rb"), "application/toml"),
        )
    )
    for path in raw_files:
        files.append(("files[]", (path.name, LazyFile(path), "application/octet-stream")))
    return files


def request_transaction(url: str, manifest: Path, raw_files: Iterable[Path]) -> ResponseData:
    files = build_multipart(manifest, raw_files)
    data = {
        "message": "rileydata-full",
        "dry_run": "false",
    }

    try:
        resp = requests.post(url, data=data, files=files, timeout=None)
    finally:
        # Close all file handles
        for _, (_, file_obj, _) in files:
            close = getattr(file_obj, "close", None)
            if callable(close):
                close()

    raw_text = resp.text
    try:
        payload = resp.json()
    except json.JSONDecodeError:
        raise RuntimeError(f"Non-JSON response (HTTP {resp.status_code}):\n{raw_text}")

    status = payload.get("status", "")
    if not status:
        raise RuntimeError("Response missing status field")
    return ResponseData(status=status, json=payload, raw_text=raw_text)


def download_parquet(compose_cmd: List[str], stack: str, parquet_key: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as fh:
        run(
            compose_cmd
            + [
                "-p",
                stack,
                "run",
                "--rm",
                "--entrypoint",
                "",
                "minio-init",
                "sh",
                "-c",
                "mc alias set --api s3v4 local http://minio:9000 minio miniosecret >/dev/null && mc cat local/sapflux/" + parquet_key,
            ]
        , check=True, stdout=fh)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    compose_cmd = shlex.split(os.environ.get("COMPOSE", "docker compose"))
    stack = "sapflux-rileydata-full"
    manifest_path = repo_root / "rileydata" / "transaction" / "meta_tx.toml"
    rawdata_dir = repo_root / "rileydata" / "rawdata"
    output_dir = repo_root / "rileydata" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not rawdata_dir.is_dir():
        raise FileNotFoundError(f"Raw data directory not found: {rawdata_dir}")

    tmp_manifest_path = Path(tempfile.mkstemp(suffix=".toml")[1])
    try:
        manifest_text = manifest_path.read_text()
        tmp_manifest_path.write_text(manifest_text.replace("[[deployments.add]]", "[[deployments]]"))

        # Start fresh stack
        run(compose_cmd + ["-p", stack, "down", "-v"], check=False)
        run(compose_cmd + ["-p", stack, "up", "-d", "--build"])
        run(compose_cmd + ["-p", stack, "wait"], check=False)

        wait_for_api("http://localhost:8080/health")

        for endpoint in ("admin/migrate", "admin/seed"):
            resp = requests.post(f"http://localhost:8080/{endpoint}", timeout=30)
            resp.raise_for_status()

        run(
            compose_cmd
            + [
                "-p",
                stack,
                "run",
                "--rm",
                "--entrypoint",
                "",
                "minio-init",
                "sh",
                "-c",
                "mc alias set --api s3v4 local http://minio:9000 minio miniosecret >/dev/null && mc mb --ignore-existing local/sapflux >/dev/null && mc anonymous set download local/sapflux >/dev/null",
            ]
        )

        raw_files = sorted(p for p in rawdata_dir.rglob("*") if p.is_file())
        if not raw_files:
            raise RuntimeError(f"No raw data files found in {rawdata_dir}")

        print(f"Posting manifest with {len(raw_files)} files")

        response = request_transaction(
            "http://localhost:8080/transactions", tmp_manifest_path, raw_files
        )

        print(json.dumps(response.json, indent=2))

        if response.status != "success":
            raise RuntimeError(f"Transaction failed with status '{response.status}'")

        artifacts = response.json.get("receipt", {}).get("artifacts")
        if artifacts and artifacts.get("parquet_key") and artifacts.get("output_id"):
            parquet_key = artifacts["parquet_key"]
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            destination = output_dir / f"full_smoke_{timestamp}.parquet"
            print(f"Downloading parquet to {destination}")
            download_parquet(compose_cmd, stack, parquet_key, destination)

        print("Smoke rileydata full test completed successfully")
    finally:
        try:
            run(compose_cmd + ["-p", stack, "down", "-v"], check=False)
        finally:
            if tmp_manifest_path.exists():
                tmp_manifest_path.unlink()


if __name__ == "__main__":
    main()
