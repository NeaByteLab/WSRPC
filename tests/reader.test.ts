import * as Assert from '@std/assert'
import Protocol from '@app/protocol.ts'
import Reader from '@app/reader.ts'
import Features from '@tests/features.ts'

Deno.test('Reader constructor - rejects an invalid frame limit', () => {
  Assert.assertThrows(
    () => new Reader(Features.scripted([]), 0),
    RangeError,
    'Frame limit must be an integer between 1 and 4294967295'
  )
})

Deno.test('Reader.readFrame - decodes one complete frame', async () => {
  const payload = { m: 'getUser', p: { id: 1 } }
  const frame = Protocol.encode(Protocol.opcode.request, 11, payload)
  const reader = new Reader(Features.scripted([frame]))
  const decoded = await reader.readFrame()
  if (decoded === null) {
    throw new TypeError('expected a frame')
  }
  Assert.assertEquals(decoded.opcode, Protocol.opcode.request)
  Assert.assertEquals(decoded.ident, 11)
  Assert.assertEquals(Protocol.decode(decoded.payload), payload)
  Assert.assertEquals(await reader.readFrame(), null)
})

Deno.test('Reader.readFrame - joins a header and payload split across reads', async () => {
  const frame = Protocol.encode(Protocol.opcode.success, 4, { ok: true })
  const reader = new Reader(Features.scripted([frame], 3))
  const decoded = await reader.readFrame()
  if (decoded === null) {
    throw new TypeError('expected a frame')
  }
  Assert.assertEquals(decoded.ident, 4)
  Assert.assertEquals(Protocol.decode(decoded.payload), { ok: true })
})

Deno.test('Reader.readFrame - reads a frame larger than the initial buffer', async () => {
  const payload = { blob: 'a'.repeat(70_000) }
  const frame = Protocol.encode(Protocol.opcode.success, 15, payload)
  const trailing = Protocol.encode(Protocol.opcode.pong, 16)
  const reader = new Reader(Features.scripted([frame, trailing], 8192))
  const decoded = await reader.readFrame()
  const follow = await reader.readFrame()
  if (decoded === null || follow === null) {
    throw new TypeError('expected both frames')
  }
  Assert.assertEquals(Protocol.decode(decoded.payload), payload)
  Assert.assertEquals(follow.opcode, Protocol.opcode.pong)
  Assert.assertEquals(follow.ident, 16)
  Assert.assertEquals(await reader.readFrame(), null)
})

Deno.test('Reader.readFrame - rejects a declared length above the frame limit', async () => {
  const reader = new Reader(Features.scripted([Features.header(Protocol.opcode.request, 1, 9)]), 8)
  await Assert.assertRejects(
    () => reader.readFrame(),
    RangeError,
    'Frame payload length 9 exceeds maximum 8'
  )
})

Deno.test('Reader.readFrame - rejects a stream that ends before the payload starts', async () => {
  const reader = new Reader(
    Features.scripted([Features.header(Protocol.opcode.request, 1, 4), null])
  )
  await Assert.assertRejects(
    () => reader.readFrame(),
    RangeError,
    'Frame payload ended before the declared length was read'
  )
})

Deno.test('Reader.readFrame - rejects a stream that ends inside the header', async () => {
  const reader = new Reader(
    Features.scripted([Features.header(Protocol.opcode.request, 1, 0).subarray(0, 4), null])
  )
  await Assert.assertRejects(
    () => reader.readFrame(),
    RangeError,
    'TCP frame ended before the declared bytes were read'
  )
})

Deno.test('Reader.readFrame - rejects a stream that ends inside the payload', async () => {
  const prefix = Features.header(Protocol.opcode.request, 1, 4)
  const partial = new Uint8Array(prefix.byteLength + 2)
  partial.set(prefix, 0)
  const reader = new Reader(Features.scripted([partial, null]))
  await Assert.assertRejects(
    () => reader.readFrame(),
    RangeError,
    'TCP frame ended before the declared bytes were read'
  )
})

Deno.test('Reader.readFrame - rejects a zero-byte socket read', async () => {
  const reader = new Reader(Features.scripted(['zero']))
  await Assert.assertRejects(
    () => reader.readFrame(),
    RangeError,
    'Zero-byte read from TCP stream'
  )
})

Deno.test('Reader.readFrame - rejects an opcode outside 1 through 6', async () => {
  const reader = new Reader(Features.scripted([Features.header(0x07, 1, 0)]))
  await Assert.assertRejects(
    () => reader.readFrame(),
    RangeError,
    'Invalid frame opcode 7'
  )
})

Deno.test('Reader.readFrame - returns an empty payload for a header-only frame', async () => {
  const reader = new Reader(Features.scripted([Protocol.encode(Protocol.opcode.pong, 8)]))
  const decoded = await reader.readFrame()
  if (decoded === null) {
    throw new TypeError('expected a frame')
  }
  Assert.assertEquals(decoded.opcode, Protocol.opcode.pong)
  Assert.assertEquals(decoded.ident, 8)
  Assert.assertEquals(decoded.payload.byteLength, 0)
})

Deno.test('Reader.readFrame - returns null on a clean end of stream', async () => {
  const reader = new Reader(Features.scripted([null]))
  Assert.assertEquals(await reader.readFrame(), null)
})

Deno.test('Reader.readFrame - returns two frames from one chunk', async () => {
  const first = Protocol.encode(Protocol.opcode.request, 1, { n: 1 })
  const second = Protocol.encode(Protocol.opcode.request, 2, { n: 2 })
  const joined = new Uint8Array(first.byteLength + second.byteLength)
  joined.set(first, 0)
  joined.set(second, first.byteLength)
  const reader = new Reader(Features.scripted([joined]))
  const firstFrame = await reader.readFrame()
  const secondFrame = await reader.readFrame()
  if (firstFrame === null || secondFrame === null) {
    throw new TypeError('expected two frames')
  }
  Assert.assertEquals(Protocol.decode(firstFrame.payload), { n: 1 })
  Assert.assertEquals(Protocol.decode(secondFrame.payload), { n: 2 })
})
