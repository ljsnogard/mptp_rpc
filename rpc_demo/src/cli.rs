//! 命令行参数：角色 + UDS 路径。
//!
//! 用法：`rpc_demo server|client [--socket /tmp/mptp_rpc_demo.sock]`。

use std::path::PathBuf;

/// 本进程扮演的角色。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    /// 监听端。
    Server,

    /// 拨号端。
    Client,
}

/// 缺省 UDS 路径。
pub fn default_socket_() -> PathBuf {
    std::env::temp_dir().join("mptp_rpc_demo.sock")
}

/// `--help` 的输出。
pub const K_HELP: &str = "\
MPTP RPC 手动实现试验：UDS 上的 workspace/session CRUD

用法：
  cargo run -p rpc_demo -- server [--socket /tmp/mptp_rpc_demo.sock]
  cargo run -p rpc_demo -- client [--socket /tmp/mptp_rpc_demo.sock]

参数：
  --socket <路径>   服务端 bind / 客户端 connect 的 UDS 路径
  -h, --help        打印本说明
";

/// 解析出来的选项。
#[derive(Clone, Debug)]
pub struct Options {
    /// 本进程角色。
    pub role: Role,

    /// UDS 路径。
    pub socket: PathBuf,
}

/// 解析命令行。
pub fn parse_args_<I>(args: I) -> Result<Options, String>
where
    I: IntoIterator<Item = String>,
{
    let mut iter = args.into_iter();
    let _program = iter.next();

    let mut role: Option<Role> = Option::None;
    let mut socket = default_socket_();

    while let Option::Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" | "--help" => return Result::Err(K_HELP.to_string()),
            "server" if role.is_none() => role = Option::Some(Role::Server),
            "client" if role.is_none() => role = Option::Some(Role::Client),
            "--socket" => {
                let value = iter
                    .next()
                    .ok_or_else(|| format!("`--socket` 缺少路径\n\n{K_HELP}"))?;
                socket = PathBuf::from(value);
            }
            other => return Result::Err(format!("不认识的参数 {other:?}\n\n{K_HELP}")),
        }
    }

    let Option::Some(role) = role else {
        return Result::Err(format!("缺少角色（server / client）\n\n{K_HELP}"));
    };
    Result::Ok(Options { role, socket })
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 把字符串数组当作命令行喂进解析器。
    fn parse_(args: &[&str]) -> Result<Options, String> {
        let mut all = vec!["rpc_demo".to_string()];
        all.extend(args.iter().map(|arg| arg.to_string()));
        parse_args_(all)
    }

    /// 测试服务端/客户端角色与 `--socket` 的解析。
    /// Test role and `--socket` parsing for both roles.
    ///
    /// - 手段：分别解析 `server --socket /tmp/x.sock` 与 `client`。
    /// - 判断：角色正确；显式路径生效；客户端缺省路径非空。
    ///
    /// - Means: parse a server with an explicit socket and a client with defaults.
    /// - Verdict: roles and the explicit path are preserved; the default path is set.
    #[test]
    fn parses_roles_and_socket_() {
        let server = parse_(&["server", "--socket", "/tmp/x.sock"]).expect("应当解析成功");
        assert_eq!(server.role, Role::Server);
        assert_eq!(server.socket, PathBuf::from("/tmp/x.sock"));

        let client = parse_(&["client"]).expect("应当解析成功");
        assert_eq!(client.role, Role::Client);
        assert!(!client.socket.as_os_str().is_empty());
    }

    /// 测试缺角色、坏参数与缺取值都被明确拒绝。
    /// Test that missing roles, unknown options and missing values are rejected.
    ///
    /// - 手段：分别给空参数、未知参数、`--socket` 缺值。
    /// - 判断：三次都返回 `Err`，且提示里带上对应说明。
    ///
    /// - Means: feed an empty command line, an unknown option, and a missing value.
    /// - Verdict: all three are errors with a useful message.
    #[test]
    fn rejects_bad_input_() {
        assert!(
            parse_(&[])
                .expect_err("缺角色应当失败")
                .contains("缺少角色")
        );
        assert!(
            parse_(&["server", "--nope"])
                .expect_err("未知参数应当失败")
                .contains("不认识的参数")
        );
        assert!(
            parse_(&["server", "--socket"])
                .expect_err("缺取值应当失败")
                .contains("缺少路径")
        );
    }
}
