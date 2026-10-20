#!/usr/bin/env python3
"""起两个独立进程，经 UDS 跑 rpc_demo 的 workspace/session CRUD。

默认先 `cargo build -p rpc_demo`，随后每一轮：
1. 起服务端（`rpc_demo server --socket ...`）；
2. 等 socket 文件出现；
3. 跑客户端；
4. 等服务端退出，并核对两端的退出码与关键输出。

`--skip-build` 复用已编好的二进制；`--rounds N` 重复多轮。
`MPTP_CARGO_OFFLINE=--offline` 会把该参数原样传给 cargo。
"""

from __future__ import annotations

import argparse
import os
import pathlib
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
BIN = ROOT / "target" / "debug" / "rpc_demo"
EXPECTED_JOBS = 11


def run_build(offline: str) -> None:
    cmd = ["cargo", "build", "-p", "rpc_demo"]
    if offline:
        cmd.append(offline)
    print("$ " + " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)


def wait_for_socket(path: pathlib.Path, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.02)
    raise RuntimeError(f"等待 socket 超时：{path}")


def run_round(index: int) -> None:
    socket = pathlib.Path(tempfile.gettempdir()) / f"mptp_rpc_demo_{os.getpid()}_{index}.sock"
    try:
        socket.unlink()
    except FileNotFoundError:
        pass

    server = subprocess.Popen(
        [str(BIN), "server", "--socket", str(socket)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_for_socket(socket)
        client = subprocess.run(
            [str(BIN), "client", "--socket", str(socket)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        server_out, _ = server.communicate(timeout=60)
    finally:
        if server.poll() is None:
            server.kill()
            server.wait(timeout=5)
        try:
            socket.unlink()
        except FileNotFoundError:
            pass

    print(f"--- round {index} ---")
    print("[server]")
    print(server_out.rstrip())
    print("[client]")
    print(client.stdout.rstrip())
    if client.stderr.strip():
        print(client.stderr.rstrip(), file=sys.stderr)

    failures = []
    if client.returncode != 0:
        failures.append(f"客户端退出码 {client.returncode}")
    if server.returncode != 0:
        failures.append(f"服务端退出码 {server.returncode}")
    if f"本轮处理 {EXPECTED_JOBS} 条请求" not in server_out:
        failures.append("服务端没有报告预期请求数")
    if "通过 ApiV2" not in client.stdout:
        failures.append("客户端没有走 ApiV2 调用")
    if failures:
        raise RuntimeError("；".join(failures))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true", help="复用已编好的二进制")
    parser.add_argument("--rounds", type=int, default=1, help="重复轮数（默认 1）")
    args = parser.parse_args()

    offline = os.environ.get("MPTP_CARGO_OFFLINE", "")
    try:
        if not args.skip_build:
            run_build(offline)
        if not BIN.exists():
            raise RuntimeError(f"找不到二进制：{BIN}")
        for index in range(1, args.rounds + 1):
            run_round(index)
    except (subprocess.SubprocessError, RuntimeError) as err:
        print(f"端到端失败：{err}", file=sys.stderr)
        return 1

    print(f"端到端通过（{args.rounds} 轮，UDS）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
