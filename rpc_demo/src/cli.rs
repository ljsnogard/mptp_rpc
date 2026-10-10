//! 命令行参数。
//!
//! 只认「一个角色 + 若干 `--键 值`」，因此不引入参数解析库——那会让这个 demo 的
//! 依赖面比它演示的东西还大。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// 本 demo 缺省的监听 / 连接地址。
pub const K_DEFAULT_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7719u16);

/// 服务端监听的 dock（与 `abs_smux` 的 dock 语义一致，只是一个会话身份编号）。
pub const K_DEFAULT_DOCK: u32 = 1u32;

/// 用法说明。
pub const K_HELP: &str = "\
MPTP 最小演示：真实 TCP socket 上的一问一答

用法：
  cargo run -p rpc_demo -- server [--listen 127.0.0.1:7719] [--dock 1]
  cargo run -p rpc_demo -- client [--peer   127.0.0.1:7719] [--dock 1]

参数：
  --listen <地址>   服务端绑定的地址（缺省 127.0.0.1:7719）
  --peer   <地址>   客户端要连的地址（缺省 127.0.0.1:7719）
  --dock   <编号>   两侧约定的 dock 编号（缺省 1）
  -h, --help        打印本说明

两个终端各跑一条命令即可：服务端先起，它会打印一行 ready；客户端随后拨号，
发出一次带请求体的 Call /rpc/echo，并把服务端回声回来的请求体打印出来。";

/// 本进程扮演的角色。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Server,
    Client,
}

/// 解析好的运行参数。
#[derive(Clone, Debug)]
pub struct Options {
    /// 本进程的角色。
    pub role: Role,

    /// 服务端绑定的地址（客户端角色下不参与）。
    pub listen: SocketAddr,

    /// 客户端要连的地址（服务端角色下不参与）。
    pub peer: SocketAddr,

    /// 两侧约定的 dock 编号。
    pub dock: u32,
}

impl Options {
    /// 按角色取「本进程要用的那个地址」。
    pub const fn target_addr(&self) -> SocketAddr {
        match self.role {
            Role::Server => self.listen,
            Role::Client => self.peer,
        }
    }
}

/// 解析一次命令行：第一个位置参数是角色，其余是 `--键 值`。
///
/// # Errors
///
/// 角色缺失或不认识、地址解析失败、dock 不是无符号整数、出现不认识的键时返回
/// 一段可直接打印给用户的说明。
pub fn parse_args_<I>(args: I) -> Result<Options, String>
where
    I: IntoIterator<Item = String>,
{
    let mut iter = args.into_iter();
    // 第一个参数按惯例是程序自身。
    let _program = iter.next();

    let mut role: Option<Role> = Option::None;
    let mut listen = K_DEFAULT_ADDR;
    let mut peer = K_DEFAULT_ADDR;
    let mut dock = K_DEFAULT_DOCK;

    while let Option::Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" | "--help" => return Result::Err(K_HELP.to_string()),
            "server" if role.is_none() => role = Option::Some(Role::Server),
            "client" if role.is_none() => role = Option::Some(Role::Client),
            "--listen" => listen = parse_addr_(&mut iter, "--listen")?,
            "--peer" => peer = parse_addr_(&mut iter, "--peer")?,
            "--dock" => {
                let text = next_value_(&mut iter, "--dock")?;
                dock = text
                    .parse::<u32>()
                    .map_err(|err| format!("--dock 需要是个无符号整数，收到 {text:?}：{err}"))?;
            }
            other => {
                return Result::Err(format!("不认识的参数 {other:?}\n\n{K_HELP}"));
            }
        }
    }

    let Option::Some(role) = role else {
        return Result::Err(format!("缺少角色（server / client）\n\n{K_HELP}"));
    };
    Result::Ok(Options {
        role,
        listen,
        peer,
        dock,
    })
}

/// 取下一个参数的值。
fn next_value_<I>(iter: &mut I, key: &str) -> Result<String, String>
where
    I: Iterator<Item = String>,
{
    iter.next()
        .ok_or_else(|| format!("{key} 后面缺少取值\n\n{K_HELP}"))
}

/// 取下一个参数并解析成 socket 地址。
fn parse_addr_<I>(iter: &mut I, key: &str) -> Result<SocketAddr, String>
where
    I: Iterator<Item = String>,
{
    let text = next_value_(iter, key)?;
    text.parse::<SocketAddr>()
        .map_err(|err| format!("{key} 需要是个 socket 地址，收到 {text:?}：{err}"))
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 把字符串数组当作命令行喂进解析器。
    fn parse_(args: &[&str]) -> Result<Options, String> {
        let mut all = vec!["rpc_demo".to_string()];
        all.extend(args.iter().map(|s| s.to_string()));
        parse_args_(all)
    }

    /// 测试服务端角色的参数解析与缺省值。
    /// - 手段：只给 `server`，再用 `--listen` / `--dock` 覆盖一次。
    /// - 判断：角色是 `Server`；不给地址时 `listen` / `peer` 都是缺省地址；给了
    ///   `--listen` 与 `--dock` 之后，解析结果就是给的那些值。
    #[test]
    fn parses_server_role_() {
        let opts = parse_(&["server"]).expect("只给角色就应当解析成功");
        assert_eq!(opts.role, Role::Server, "角色应当是服务端");
        assert_eq!(opts.listen, K_DEFAULT_ADDR, "缺省监听地址应当生效");
        assert_eq!(opts.dock, K_DEFAULT_DOCK, "缺省 dock 应当生效");

        let opts = parse_(&["server", "--listen", "0.0.0.0:9000", "--dock", "7"])
            .expect("带取值的参数应当解析成功");
        assert_eq!(opts.listen, "0.0.0.0:9000".parse().expect("地址应当合法"));
        assert_eq!(opts.dock, 7u32, "dock 应当被覆盖");
        assert_eq!(
            opts.target_addr(),
            "0.0.0.0:9000".parse().expect("地址应当合法"),
            "服务端的目标地址就是它自己的监听地址"
        );
    }

    /// 测试客户端角色的参数解析。
    /// - 手段：给 `client` 与一个 `--peer`。
    /// - 判断：角色是 `Client`，`peer` 是给的值，且 `target_addr()` 取的就是 `peer`
    ///   而不是 `listen`。
    #[test]
    fn parses_client_role_() {
        let opts = parse_(&["client", "--peer", "127.0.0.1:8888"]).expect("应当解析成功");
        assert_eq!(opts.role, Role::Client, "角色应当是客户端");
        assert_eq!(opts.peer, "127.0.0.1:8888".parse().expect("地址应当合法"));
        assert_eq!(
            opts.target_addr(),
            "127.0.0.1:8888".parse().expect("地址应当合法"),
            "客户端的目标地址是它要连的地址"
        );
    }

    /// 测试坏输入都被明确拒绝。
    /// - 手段：分别给出「没有角色」「不认识的键」「地址非法」「dock 不是整数」
    ///   「键后面没有取值」五种命令行。
    /// - 判断：五种都返回 `Err`；前两种的提示里带上用法说明，便于直接打印给用户。
    #[test]
    fn rejects_bad_input_() {
        let none_role = parse_(&[]).expect_err("没有角色应当失败");
        assert!(
            none_role.contains("缺少角色"),
            "提示应当说明缺什么：{none_role}"
        );

        let unknown = parse_(&["server", "--nope"]).expect_err("不认识的键应当失败");
        assert!(
            unknown.contains("不认识的参数"),
            "提示应当指出参数：{unknown}"
        );
        assert!(unknown.contains("用法"), "不认识的参数应当附带用法说明");

        let bad_addr = parse_(&["client", "--peer", "not-an-addr"]).expect_err("地址非法应当失败");
        assert!(
            bad_addr.contains("--peer"),
            "提示应当指出是哪个键：{bad_addr}"
        );

        let bad_dock = parse_(&["server", "--dock", "x"]).expect_err("dock 非整数应当失败");
        assert!(
            bad_dock.contains("--dock"),
            "提示应当指出是哪个键：{bad_dock}"
        );

        let no_value = parse_(&["server", "--listen"]).expect_err("缺少取值应当失败");
        assert!(
            no_value.contains("缺少取值"),
            "提示应当说明缺取值：{no_value}"
        );
    }

    /// 测试帮助参数直接给出用法说明。
    /// - 手段：给 `--help`。
    /// - 判断：返回的 `Err` 内容就是用法说明本身（调用方据此打印并成功退出）。
    #[test]
    fn help_returns_usage_() {
        let help = parse_(&["--help"]).expect_err("--help 走的是「打印说明」这条路");
        assert_eq!(help, K_HELP, "应当原样给出用法说明");
    }
}
