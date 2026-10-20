#!/usr/bin/env python3
"""真实 socket 端到端：起两个进程通信，并核对两端的类型标识确实一致。

一次「用例」= 两个**独立进程**（`mptp_smux_v1_demo server` 与 `client`）+ 一条 TCP 连接
+ 三次调用。本脚本负责：

1. 挑一个空闲端口，先起服务端，**等它打出「等待客户端连接」**再起客户端；
2. 等两端退出，收下各自的输出；
3. 做**跨进程核对**——这是本脚本存在的理由：
   - 客户端看到的回复标识，必须与服务端打印的「回 XXXReply（标识 0x..）」逐位相同；
   - 三次调用都要出现，且第三次必须是 406；
   - 服务端必须报「共处理 3 条子流」。
4. 全部断言通过才退出 0；任一条不过就打出两端原始输出并退出非 0。

用法：

    python3 mptp_smux_v1_demo/scripts/e2e_socket.py          # 编一次，跑一轮
    python3 mptp_smux_v1_demo/scripts/e2e_socket.py --rounds 3
    python3 mptp_smux_v1_demo/scripts/e2e_socket.py --skip-build

`MPTP_CARGO_OFFLINE=--offline` 会被原样转给 cargo（本机 cargo 缓存只读 / 断网时用）。
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import socket
import subprocess
import sys
import time

REPO_DIR = pathlib.Path(__file__).resolve().parent.parent.parent
TARGET_DIR = REPO_DIR / "target" / "debug"
BIN = TARGET_DIR / "mptp_smux_v1_demo"

# 一轮里最多等多久（秒）：服务端起完、客户端跑完、两端退出。
K_ROUND_TIMEOUT = 30.0

# 服务端「已就绪」的信号行（客户端必须在它之后才起）。
K_SERVER_READY = "等待客户端连接"

# 服务端收尾的信号行。
K_SERVER_DONE = "退出。"


def pick_free_port() -> int:
    """让内核挑一个空闲端口（先占后放；本机环回场景足够）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_round(round_index: int, offline: str) -> tuple[bool, str]:
    """跑一轮；返回 (是否通过, 失败说明)。"""
    port = pick_free_port()
    addr = f"127.0.0.1:{port}"

    server = subprocess.Popen(
        [str(BIN), "server", "--listen", addr],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        # 等服务端打出就绪行：直接读它的 stdout（不阻塞地轮询）。
        ready = False
        server_lines: list[str] = []
        deadline = time.monotonic() + K_ROUND_TIMEOUT
        while time.monotonic() < deadline:
            line = server.stdout.readline() if server.stdout else ""
            if line:
                server_lines.append(line)
                if K_SERVER_READY in line:
                    ready = True
                    break
            elif server.poll() is not None:
                break
        if not ready:
            return False, f"第 {round_index} 轮：服务端没有进入就绪状态"

        client = subprocess.run(
            [str(BIN), "client", "--peer", addr],
            capture_output=True,
            text=True,
            timeout=K_ROUND_TIMEOUT,
        )
        if client.returncode != 0:
            return False, (
                f"第 {round_index} 轮：客户端退出码 {client.returncode}\n{client.stdout}{client.stderr}"
            )

        # 收下服务端剩下的输出（读到它退出为止）。
        try:
            rest, _ = server.communicate(timeout=K_ROUND_TIMEOUT)
        except subprocess.TimeoutExpired:
            server.kill()
            rest, _ = server.communicate()
            return False, f"第 {round_index} 轮：服务端没有自己退出"
        server_out = "".join(server_lines) + (rest or "")
        client_out = client.stdout

        problems = check_outputs_(server_out, client_out)
        if problems:
            return False, (
                f"第 {round_index} 轮：{problems}\n"
                f"===== 服务端 =====\n{server_out}\n===== 客户端 =====\n{client_out}"
            )
        return True, ""
    finally:
        if server.poll() is None:
            server.kill()
            server.communicate()


def check_outputs_(server_out: str, client_out: str) -> str:
    """核对两端输出；返回空串表示全部通过，否则返回失败说明。"""
    # 1. 客户端三次调用都出现。
    for needle in ("/rpc/echo 回来", "/rpc/sum 回来", "服务端回 406"):
        if needle not in client_out:
            return f"客户端输出缺少 `{needle}`"

    # 2. 服务端收尾行。
    if K_SERVER_DONE not in server_out:
        return "服务端没有打印收尾行"
    if "共处理 3 条子流" not in server_out:
        return "服务端处理的子流条数不是 3"

    # 3. **跨进程**核对类型标识：客户端看到的回复标识必须是服务端打印的那个。
    #    两端各自用类型名算出的哈希必须逐位相同——否则「标识由类型统一给出」就只是
    #    同一个进程里的巧合。
    pairs = [
        ("EchoReply", r"客户端：/rpc/echo 回来 .*（回复声明 (0x[0-9a-f]+)）"),
        ("SumReply", r"客户端：/rpc/sum 回来 .*（回复声明 (0x[0-9a-f]+)）"),
    ]
    for type_name, pattern in pairs:
        client_match = re.search(pattern, client_out)
        if client_match is None:
            return f"客户端没有打印 {type_name} 的标识"
        server_match = re.search(rf"回 {type_name}（标识 (0x[0-9a-f]+)）", server_out)
        if server_match is None:
            return f"服务端没有打印 {type_name} 的标识"
        if client_match.group(1) != server_match.group(1):
            return (
                f"{type_name} 的标识两端不一致：客户端 {client_match.group(1)}，"
                f"服务端 {server_match.group(1)}"
            )

    # 4. 错配那一次必须是 406，而不是「解出乱码」或成功。
    if "服务端：对端声明" not in server_out or "回 406（不解码）" not in server_out:
        return "服务端没有按类型不符拒绝那条错配请求"

    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="mptp_smux_v1_demo 的真实 socket 端到端")
    parser.add_argument("--rounds", type=int, default=1, help="跑几轮（每轮换一个端口）")
    parser.add_argument("--skip-build", action="store_true", help="复用已经编好的二进制")
    args = parser.parse_args()

    offline = os.environ.get("MPTP_CARGO_OFFLINE", "")
    if not args.skip_build:
        cmd = ["cargo", "build", "-p", "mptp_smux_v1_demo"]
        if offline:
            cmd.append(offline)
        built = subprocess.run(cmd, cwd=REPO_DIR, capture_output=True, text=True)
        if built.returncode != 0:
            print("构建失败：")
            print(built.stdout)
            print(built.stderr)
            return 1

    if not BIN.exists():
        print(f"找不到二进制 {BIN}（先去掉 --skip-build 编一次）")
        return 1

    for index in range(1, args.rounds + 1):
        ok, detail = run_round(index, offline)
        if not ok:
            print(detail)
            return 1
        print(f"第 {index} 轮通过：三次调用 + 跨进程标识一致 + 406 拒绝")

    print(f"全部通过（{args.rounds} 轮）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
