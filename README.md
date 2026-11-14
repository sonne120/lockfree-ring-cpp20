# lockfree-ring-cpp20

Header-only ring buffers in C++20: plain queues and the LMAX Disruptor.

## What's inside

| Type | Header | Use |
|---|---|---|
| `ring::queue::CircularBuffer` | `ring/queue/ring_buffer.hpp` | single thread |
| `ring::queue::SpscQueue` | `ring/queue/spsc_queue.hpp` | 1 producer, 1 consumer |
| `ring::queue::MpmcQueue` | `ring/queue/mpmc_queue.hpp` | many producers, many consumers |
| `ring::disruptor::RingBuffer` | `ring/disruptor/disruptor.hpp` | every consumer sees every event |

## Usage

```cpp
#include <ring/disruptor/disruptor.hpp>
using namespace ring::disruptor;

RingBuffer<Event> rb(1024);
BatchConsumer consumer(rb, [](Event& e, std::int64_t seq, bool end_of_batch) {});
rb.set_gating_sequences({&consumer.sequence()});
std::thread t([&] { consumer.run(); });

const auto seq = rb.claim();
rb[seq] = Event{};
rb.publish(seq);
```

More in `examples/`.

## Build

```
cmake -S . -B build
cmake --build build
ctest --test-dir build
```

## Benchmark

```
./build/queue_vs_disruptor
```
