# rpc_core_v1

The core library of RPC over `MPTP` based on `smux_v1`.

MPTP = Multiplexed P2P Transportation Protocol.

## Status

**Phase 1 (runtime for `#[rpc]`)**: the crate provides the pieces both macro forms expand
against:

- `RpcError` / `RpcResult` with a business-status mapping;
- `TrRpcInterface` and `/{interface}/{service}` path composition;
- the minimal `ClientProxy`, `TrRpcClientExt` and a generic unary call;
- the per-call serving context `RpcCallCtx` (decode → reply → close);
- `RpcRouter<I>`: the inline interface-level router; users register concrete services with
  `add_service::<Impl>(...)`, and `#[rpc(service_of(I))]` adds the required
  `TrRpcService` impl;
- the queued legacy chain `RpcJob<C>` / `ServiceRoute<C>` / `TrRpcServiceRoute` /
  `TrRpcRouter` / `RpcRouterHandler`; the concrete two-level trie router lives in the
  separate `rpc_router_trie` crate;
- streaming MessagePack body IO: a bounded SPSC ring feeds `send_req_body_async` / chunked
  reply writers, with no whole-body staging buffer.

The service/interface macros only generate the wire contract, the client proxy and the
serving dispatch; server organization is explicit user code (`Router` +
`RpcRouter::add_service`). The MPTP wire format lives in `mptp_smux`; body framing lives in
`abs_mptp`; neither is reimplemented here.

## Prelude

Most protocol / client / server code can start with one glob instead of a long list of
paths:

```rust
use rpc_core_v1::preludes::*;
```

It re-exports the configuration types, client proxy traits, `RpcRouter` / `Router` /
`MptpServer` / `MptpClient`, cancellation traits, `Shared` / `CoreAlloc` / `TrConnection`, the
unified error / result types, and the `gen_may_cancel_future` / `TrEncodable` / `TrDecodable`
macros. Re-exports only; no
visibility or behavior changes.

## What?

There are many protocols can carry RPC payload, for example, `gRPC` over `HTTP/2`.

Here we implement a P2P RPC, which has no official name yet, for now we call it `MPTP Rpc`.

`MPTP` is a binary protocol that requires P2P style connection (e.g. `smux_v1`)
That means, talkers of `MPTP` on both ends of the connection, can equally send requests and host
services at the same time.

So the RPC over `MPTP` is capable of serving both resource and tool calls, 
at the same time, dynamically, for both sides of the connection.

That's suitable for smart clients and servers, AI agents, especially.

For AI agents capable of programming, they can generate programs in a short while, and then use
the compiled program to handle large amount of data.

So they need dynamic hosting services and requesting resources.
The traditional TCP/IP network and C/S communication model will not suffice.
