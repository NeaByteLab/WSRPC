import * as Assert from '@std/assert'
import Queue from '@app/queue.ts'
import Features from '@tests/features.ts'

Deno.test('Queue.enqueue - does not stall after a write throws', async () => {
  let calls = 0
  const socket = Features.writer((chunk) => {
    calls += 1
    if (calls === 1) {
      return Promise.reject(new TypeError('socket reset'))
    }
    return Promise.resolve(chunk.byteLength)
  })
  const queue = new Queue()
  await Assert.assertRejects(
    () => queue.enqueue(socket, new Uint8Array([1])),
    TypeError,
    'socket reset'
  )
  await queue.enqueue(socket, new Uint8Array([2]))
})

Deno.test('Queue.enqueue - keeps later frames behind an earlier write', async () => {
  const gate = Features.gate()
  const order: number[] = []
  const socket = Features.writer((chunk) => {
    const marker = chunk[0] ?? 0
    if (marker === 1) {
      return gate.promise.then(() => {
        order.push(marker)
        return chunk.byteLength
      })
    }
    order.push(marker)
    return Promise.resolve(chunk.byteLength)
  })
  const queue = new Queue()
  const first = queue.enqueue(socket, new Uint8Array([1]))
  const second = queue.enqueue(socket, new Uint8Array([2]))
  await Promise.resolve()
  Assert.assertEquals(order, [])
  gate.release()
  await first
  await second
  Assert.assertEquals(order, [1, 2])
})

Deno.test('Queue.enqueue - rejects a zero-byte write and still accepts the next frame', async () => {
  let calls = 0
  const socket = Features.writer((chunk) => {
    calls += 1
    if (calls === 1) {
      return Promise.resolve(0)
    }
    return Promise.resolve(chunk.byteLength)
  })
  const queue = new Queue()
  await Assert.assertRejects(
    () => queue.enqueue(socket, new Uint8Array([1, 2])),
    RangeError,
    'Failed to write frame bytes to TCP stream'
  )
  await queue.enqueue(socket, new Uint8Array([3]))
  Assert.assertEquals(calls, 2)
})

Deno.test('Queue.enqueue - resolves an empty frame without a socket write', async () => {
  let calls = 0
  const socket = Features.writer(() => {
    calls += 1
    return Promise.resolve(0)
  })
  await new Queue().enqueue(socket, new Uint8Array(0))
  Assert.assertEquals(calls, 0)
})

Deno.test('Queue.enqueue - writes a frame in order across short socket writes', async () => {
  const sent: number[] = []
  const socket = Features.writer((chunk) => {
    const length = Math.min(2, chunk.byteLength)
    for (const byte of chunk.subarray(0, length)) {
      sent.push(byte)
    }
    return Promise.resolve(length)
  })
  await new Queue().enqueue(socket, new Uint8Array([1, 2, 3, 4, 5]))
  Assert.assertEquals(sent, [1, 2, 3, 4, 5])
})

Deno.test('Queue.reset - lets the next write skip the dropped chain', async () => {
  const gate = Features.gate()
  let started = 0
  const socket = Features.writer(() => {
    started += 1
    if (started === 1) {
      return gate.promise.then(() => 1)
    }
    return Promise.resolve(1)
  })
  const queue = new Queue()
  const first = queue.enqueue(socket, new Uint8Array([1]))
  queue.reset()
  await queue.enqueue(socket, new Uint8Array([2]))
  Assert.assertEquals(started, 2)
  gate.release()
  await first
})
