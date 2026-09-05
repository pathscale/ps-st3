# The core-local bypass slot: what it is, and why it is not on `main`

A one-item `Cell<Option<T>>` in `lifo::Worker`, read by `pop` ahead of the ring.
The owner holds `Worker` by construction, so it costs no atomic: a predictable
not-taken branch against a full `compare_exchange` on the ring path.

`push_next` displaces whatever it holds onto the ring rather than dropping it,
so at most one item is ever hidden. `drain_next` puts that item back where a
stealer can reach it, and `is_empty` counts it, because its documented contract
is that a true means the next `pop` fails.

This is not a novel idea. Go has `runnext`, Tokio has `lifo_slot`, both one item
in front of the real queue. They split on the hazard:

| | stealable | owner pays | consequence |
|---|---|---|---|
| Go `runnext` | yes, after a ~3 µs thief spin | an atomic on every `runqget` | no starvation |
| Tokio `lifo_slot` | no | nothing | shipped `disable_lifo_slot` as a kill switch |

Ours is Tokio's position. The saving exists **because** the item is invisible.

## Measured, 2026-09-05, M4 Max, 16 cores

Single-threaded, no contention, no stealing, 2M push+pop pairs, min of 9:

| payload | ring | jump | delta | null |
|---|---:|---:|---:|---:|
| 64-byte | 9.38 ns | 7.33 ns | **+21.9%** | -0.1% |
| u64 | 9.69 ns | 0.25 ns | +97.4% | -0.0% |

The u64 row is not a result: at 0.25 ns per pair the optimizer has folded the
`Cell` into a register, because a bypass slot inlines where a CAS does not.

Under load it does not survive. Twelve configurations across three harnesses,
each against a null of identical code run under a second label:

| harness | effect | null | verdict |
|---|---|---|---|
| fork-join, 100 ns / 1 µs / 3 µs | +0.2 / +0.9 / +2.1% | +6.7 / +3.8 / +0.4% | null exceeds effect |
| 20 producers, ps-spsc intake, 100 ns | -2.3% | +0.7% | real, negative |
| 20 producers, 3 µs | -10.6% | -9.8% | null exceeds effect |
| `Box<dyn FnOnce>` payload, DRAM-bound, 4 / 32 / 256 / 2048 KiB tiles | +3.9 / -5.5 / -0.0 / +0.2% | +0.7 / -0.7 / -3.7 / +3.2% | one positive, one negative |
| same, cache-resident, 0.25 / 1 / 4 / 16 MiB footprint | +23.7 / -2.2 / +2.8 / -0.5% | +20.7 / -1.3 / -0.1 / -0.2% | one clears, the rest do not |

Nine of twelve sit inside their own null. Of the three that clear it, one is
+3.9%, one +2.8%, and one **-5.5%**. A real effect does not change sign with
tile size. Making the working set cache-resident, the condition under which the
locality argument had to appear if ever, did not produce it.

## Why, structurally

The slot removes one `compare_exchange` at the bottom of a three-hop path:

```
producer → intake   a lock, or an SPSC ring crossing
intake   → ring     one batch per crossing
ring     → thief    segmented copy
ring     → owner    one CAS per item     <- what the slot removes
```

It optimises the cheapest hop. The measured wins are all in bulk transfer at the
hops above it: the segmented steal on this same branch is +4.2% at 3 µs and
+9.6% at 16 µs on our shape.

## What would make this live again

The saving is fixed at roughly one CAS, 2 ns on a `u64` and ~7 ns through st3's
own pop. What changes is the denominator. Against a 2,700 ns task it is 0.07%
and unmeasurable; against a 100 ns task it is 3-7%.

**Trigger to re-measure: task duration under ~700 ns**, where one CAS clears a
1% bar. Under ~200 ns it is likely decisive, and at that point the design
question is Go's rather than Tokio's, because the starvation hazard stops being
theoretical. The harness that produced the table above lives in the session
scratchpad; its nulls are calibrated and four separate harness bugs are out of
it (a hung termination check, a 41.67x latency conversion error, a global mutex
inside the rayon arm, and a start timestamp taken on a descheduled main thread).

## Validation as it stands

Six tests, each shown to fail before it was kept:

```
pop ignores the slot                -> 4 failed
push_next drops the displaced item  -> 2 failed
is_empty ignores the held item      -> 1 failed
drain_next does nothing             -> 2 failed
reverted                            -> 28 passed, 0 failed
```

`cargo test --release` 28 pass. `cargo +nightly miri test --test integration`
28 pass in 358.63 s, and under Miri the test values are `Box`, so that also
rules out a leak or double-drop of an item held in the slot when the `Worker`
drops. Loom 12 pass.
