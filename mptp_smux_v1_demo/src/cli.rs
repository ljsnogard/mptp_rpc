//! 命令行参数：角色 + 两个地址 + 起始垛口。
//!
//! 用法是 `mptp_smux_v1_demo server|client [--listen ..] [--peer ..] [--dock n]`；两端各自
//! 从 `--dock` 起**逐个**使用垛口（每条子流一个，理由见 `server_::serve_`）。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// 缺省端口。
pub const K_DEFAULT_PORT: u16 = 7719u16;

/// 缺省起始垛口。
pub const K_DEFAULT_DOCK: u32 = 1u32;

/// `--help` 的输出（也是它「失败」时带出的内容）。
pub const K_HELP: &str = "\
用法：
  mptp_smux_v1_demo server [--listen 127.0.0.1:7719] [--dock 1]
  mptp_smux_v1_demo client [--peer 127.0.0.1:7719] [--dock 1]

角色：
  server   监听并处理三条子流（echo / sum / 故意错配的那条）
  client   依次发出三条请求并核对回复

选项：
  --listen <addr>  服务端监听地址（缺省 127.0.0.1:7719）
  --peer   <addr>  客户端拨号地址（缺省 127.0.0.1:7719）
  --dock   <n>     起始垛口（缺省 1；三条子流依次用它、它 +1、它 +2）
  -h, --help       显示本说明
";

/// 本进程扮演的角色。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    /// 监听端。
    Server,

    /// 拨号端。
    Client,
}

/// 解析出来的选项。
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// 本进程的角色。
    pub role: Role,

    /// 服务端监听地址。
    pub listen: SocketAddr,

    /// 客户端拨号地址。
    pub peer: SocketAddr,

    /// 起始垛口。
    pub dock: u32,
}

impl Options {
    /// 本角色真正要用的那个地址。
    pub const fn target_addr(&self) -> SocketAddr {
        match self.role {
            Role::Server => self.listen,
            Role::Client => self.peer,
        }
    }
}

/// 解析命令行。
///
/// # Errors
///
/// 角色缺失 / 不认识、选项缺参数、地址或垛口解析失败时返回错误文案；`--help` 也走这条路
/// （它带出的就是用法说明本身，调用方据 [`K_HELP`] 判定这是不是一次正常退出）。
pub fn parse_args_(args: impl Iterator<Item = String>) -> Result<Options, String> {
    let mut role: Option<Role> = Option::None;
    let mut listen = default_addr_();
    let mut peer = default_addr_();
    let mut dock = K_DEFAULT_DOCK;

    let mut args = args.peekable();
    while let Option::Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Result::Err(K_HELP.to_string()),
            "server" => role = Option::Some(Role::Server),
            "client" => role = Option::Some(Role::Client),
            "--listen" => {
                let value = args.next().ok_or_else(|| "`--listen` 缺少地址".to_string())?;
                listen = parse_addr_(&value)?;
            }
            "--peer" => {
                let value = args.next().ok_or_else(|| "`--peer` 缺少地址".to_string())?;
                peer = parse_addr_(&value)?;
            }
            "--dock" => {
                let value = args.next().ok_or_else(|| "`--dock` 缺少数值".to_string())?;
                dock = value
                    .parse::<u32>()
                    .map_err(|err| format!("`--dock` 不是合法垛口（{value}）：{err}"))?;
            }
            other => return Result::Err(format!("不认识的参数：{other}\n\n{K_HELP}")),
        }
    }

    let role = role.ok_or_else(|| format!("必须指定角色（server / client）\n\n{K_HELP}"))?;
    Result::Ok(Options {
        role,
        listen,
        peer,
        dock,
    })
}

/// 缺省地址：`127.0.0.1:7719`。
const fn default_addr_() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), K_DEFAULT_PORT)
}

/// 解析地址；只给端口时补 `127.0.0.1`。
fn parse_addr_(text: &str) -> Result<SocketAddr, String> {
    if let Result::Ok(addr) = text.parse::<SocketAddr>() {
        return Result::Ok(addr);
    }
    let port = text
        .parse::<u16>()
        .map_err(|err| format!("`{text}` 既不是地址也不是端口：{err}"))?;
    Result::Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
}

/// 把 `anyhow` 的错误转成 CLI 的文案（只在 `main` 里用）。
pub fn format_err_(err: anyhow::Error) -> String {
    format!("{err:#}")
}
