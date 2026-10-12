# Tower with MemoryStore

This is the smallest complete example. It defines a `KeyExtractor`, creates an explicit
process-local `FixedWindowStore`, constructs a policy, and composes the resulting Layer around a
Tower service.

```sh
cargo run --example tower_memory --features memory
```

## Complete source

```rust,ignore
{{#include ../../examples/tower_memory.rs}}
```

The static key deliberately puts every request in the same bucket. Replace it with an
application-owned caller identity in a real service.
