# rpc_core_v1

The core library of RPC over `MPTP` based on `smux_v1`.

MPTP = Multiplexed P2P Transportation Protocol.

## Status

**Phase 1 (runtime for `#[rpc]` + typed Pull / Push)**: the crate provides the pieces both
macro forms expand against:

- `RpcError` / `RpcResult` with a business-status mapping;
- `TrRpcInterface` and `/{interface}/{service}` path composition;
- the minimal `ClientProxy`, `TrRpcClientExt` and a generic unary call;
- the client `open_pull_async` / `open_push_async` typed-stream entries;
- the serving body helpers (`decode_body_async` / `write_ok_async` /
  `write_stream_item_async`) and typed reply helper `typed_response`;
- `stream` — typed Pull / Push over any byte reader / writer:
  - `TypedJustFlux` / `Just` for the read side;
  - `TypedSlotFlux` / `Slot` for the write side; `Slot::set_async` encodes directly into
  the stream's own `TrBuffWrite` view (no caller-provided buffer, no 64 KiB staging);
  - the `Data_Type_Id` / `Accept` / `Body_Type` type-header protocol;
  - `RpcDataTypeInfo` / `RpcEndpointInfo` endpoint metadata markers.
- streaming MessagePack body IO: a bounded SPSC ring feeds `send_req_body_async` / chunked
  reply writers, with no whole-body staging buffer.

The MPTP wire format lives in `mptp_smux` / `mptp_wire`; body framing lives in `abs_mptp`;
neither is reimplemented here. `mptp_wire` / `mptp_smux` stay byte-only: `Just` / `Slot` and
denco never appear there.

## Prelude

Most protocol / client / server code can start with one glob instead of a long list of
paths:

```rust
use rpc_core_v1::preludes::*;
```

It re-exports the configuration types, client proxy traits, `MptpClient` / `MptpServlet`,
cancellation traits, `Shared` / `CoreAlloc` / `TrConnection`, the unified error / result
types, the typed-stream guards and type-id contract, and the
`gen_may_cancel_future` / `TrEncodable` / `TrDecodable` macros. Re-exports only; no
visibility or behavior changes.

## Typed Pull / Push

A suffix stream is raw bytes at the transport layer. `rpc_core_v1::stream` adds the object
boundary on top of denco, with one-shot guards:

```rust
use rpc_core_v1::preludes::*;

// Read side (client Pull response / server Push request suffix).
let mut flux = TypedJustFlux::<_, ChatMessage>::new(reader);
let mut iter = flux.pull_async().await;
while let Ok(just) = iter.next_async().await {
    let message: ChatMessage = just.take();
    // ...
}

// Write side (server Pull response / client Push request suffix).
let mut flux = TypedSlotFlux::<_, ChatMessage>::new(writer);
let mut iter = flux.pull_async().await;
while let Ok(slot) = iter.next_async().await {
    slot.set_async(message.clone()).await?; // encode + write one chunk
}
iter.finish_async().await?; // finite Push: close, then read the reply
```

Guarantees:

- the next item is rejected while a guard is still alive (`GuardPending`);
- dropping an untaken `Just` / unset `Slot` aborts the stream instead of silently skipping;
- the guards carry the stream lifetime and cannot be stored as `'static`;
- a finite Push is the only supported Push: the client closes the request suffix, then reads
  the response. Infinite Push / full duplex needs a different session API and is not part of
  this phase.

The client entries are `client::open_pull_async` and `client::open_push_async`; the server
uses `typed_response(ctx.session_mut(), status, method, &info)` to write the reply headers.
`open_pull_async` also checks the response prefix (`Body_Type` / `Data_Type_Id`) against the
request's `Accept`; a mismatch is reported as `RpcError::Decode` instead of handing a wrongly
typed suffix to the flux.

## Type headers

One data channel per direction:

| Method / direction | `Body_Type` | `Data_Type_Id` | `Accept` |
| --- | --- | --- | --- |
| `Call` / `Post` request | request body codec | request body type | expected response codec + type (optional) |
| `Call` / `Post` response | response body codec | response body type | — |
| `Pull` request | response suffix codec | absent (no request body) | expected suffix item codec + type (required) |
| `Pull` response | suffix codec | suffix item type | — |
| `Push` request | request suffix codec | request suffix item type | expected response codec + type (optional) |
| `Push` response | response body codec (if any) | response body type (if any) | — |

`Accept` is a single MIME value plus an optional 64-bit id:

```text
application/x-mptp-msgpack; type-id=0x1234
```

A typed endpoint defaults to `TypePolicy::Strict`: a missing required header is `400 Bad
Request`; a codec / type-id / `Accept` mismatch is `406 Not Acceptable`. `Status` has no
415, and this crate does not invent one. `TypePolicy::Optional` only relaxes *missing*
headers, never mismatches.

### Type id rules

`DataTypeId` is an explicit, stable `u64`:

- `0x0000_0000_0000_0000` is reserved for "untyped / absent" and must never go on the wire;
- applications allocate `0x0000_0000_0000_0001` – `0x7FFF_FFFF_FFFF_FFFF` explicitly;
- the high half is reserved for future generated / experimental ids;
- implement `TrDataTypeId` for your type and write the id down in the protocol crate;

```rust
use rpc_core_v1::stream::{DataTypeId, TrDataTypeId};

pub struct ChatMessage;

impl TrDataTypeId for ChatMessage {
    const DATA_TYPE_ID: DataTypeId = DataTypeId::new(0x1001).unwrap();
}
```

Versioning:

- renaming the Rust type without changing the wire shape keeps the id;
- adding / removing / reordering / retyping fields changes the id;
- an id is never reused, and one id maps to one (codec, shape).

Do **not** use `core::any::TypeId`, a type-name hash, or a runtime registry: those values are
not stable across processes or versions.

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
