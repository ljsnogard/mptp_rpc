# mptp_rpc 的配方。
#
# 默认（`just`）就是跑完所有配方：`just test`。
# 超时纪律：单元测试 60 秒；真实 socket / UDS 端到端 120 秒。
set shell := ["bash", "-uc"]

repo := justfile_directory()
offline := env('MPTP_CARGO_OFFLINE', '')
# 同一个开关也导出给 python 脚本：它会把值原样转给 cargo。
export MPTP_CARGO_OFFLINE := offline

# 默认：跑完所有配方
default: test

# 跑完所有配方：单元测试 → 协议层集成测试 → 静态检查 → 两个端到端 demo
test: unit abs-mptp clippy e2e-scene e2e-rpc

# 单元测试 + doctest（60 秒上限）
unit:
    timeout 60 cargo test --manifest-path {{repo}}/Cargo.toml {{offline}} --workspace

# `abs_mptp` 的协议层集成测试（在独立测试仓 `abs_mptp-tests` 里）。
#
# 后端选择在**测试仓**：它显式 `abs_art-facade` 关默认、只点亮 `rt-tokio`，因此用例入口
# 固定 `#[tokio::test]`。`abs_mptp` 本身不带任何 `rt-*` feature——中间层不替使用者选后端。
# 要换后端，改 `abs_mptp-tests/Cargo.toml` 那一行并同步入口属性。
abs-mptp:
    timeout 120 cargo test --manifest-path {{repo}}/Cargo.toml {{offline}} -p abs_mptp-tests
    timeout 120 cargo test --manifest-path {{repo}}/Cargo.toml {{offline}} -p abs_mptp --doc

# 静态检查
clippy:
    cargo clippy --manifest-path {{repo}}/Cargo.toml {{offline}} --workspace --all-targets

# 场景 demo：进程内起真实服务端 + 两个真实客户端（headless）。
e2e-scene:
    timeout 120 cargo run --manifest-path {{repo}}/Cargo.toml {{offline}} -p mptp_smux_v1_demo --bin scene-smoke

# 兼容旧名字：指向场景 smoke。
e2e-smux: e2e-scene

# 手动起服务端（另一个终端再跑 `scene-client`）
scene-server:
    cargo run --manifest-path {{repo}}/Cargo.toml {{offline}} -p mptp_smux_v1_demo --bin scene-server

# 编译检查 Bevy 客户端（不启动窗口）
bevy-check:
    cargo check --manifest-path {{repo}}/Cargo.toml {{offline}} -p mptp_smux_v1_demo --features bevy-client --bin scene-client

# `rpc_demo`：UDS 上的 MPTP RPC 手动实现（两级 Trie + 队列 worker）。
e2e-rpc:
    timeout 120 python3 {{repo}}/rpc_demo/scripts/e2e_uds.py

# 同上，但复用已经编好的二进制
e2e-rpc-fast:
    timeout 120 python3 {{repo}}/rpc_demo/scripts/e2e_uds.py --skip-build

# 多跑几轮：`just e2e-rpc-rounds 3`
e2e-rpc-rounds rounds:
    timeout 300 python3 {{repo}}/rpc_demo/scripts/e2e_uds.py --rounds {{rounds}}

# 兼容旧名字：`e2e` 指向 rpc_demo 的 UDS 端到端。
e2e: e2e-rpc

# 同上，复用二进制
e2e-fast: e2e-rpc-fast

# 多跑几轮
e2e-rounds rounds:
    timeout 300 python3 {{repo}}/rpc_demo/scripts/e2e_uds.py --rounds {{rounds}}

# `just test` 的别名
all: test

# 打印一份真实往返的输出，供人工看
show:
    @python3 {{repo}}/rpc_demo/scripts/e2e_uds.py --rounds 1 --skip-build
