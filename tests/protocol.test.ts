import * as Assert from '@std/assert'
import Protocol from '@app/protocol.ts'
import Features from '@tests/features.ts'

Deno.test('Protocol.configure - ignores a socket without TCP tuning methods', () => {
  Protocol.configure({} as Deno.Conn)
})

Deno.test('Protocol.configure - swallows tuning failures', () => {
  const socket = {
    setNoDelay(): void {
      throw new TypeError('nodelay refused')
    },
    setKeepAlive(): void {
      throw new TypeError('keepalive refused')
    }
  } as unknown as Deno.Conn
  Protocol.configure(socket)
})

Deno.test('Protocol.configure - tunes an open TCP socket', async () => {
  const listener = Features.listen()
  const accepted = listener.accept()
  const client = await Deno.connect({
    hostname: '127.0.0.1',
    port: listener.addr.port,
    transport: 'tcp'
  })
  const remote = await accepted
  try {
    Protocol.configure(client)
    Protocol.configure(remote)
  } finally {
    client.close()
    remote.close()
    listener.close()
  }
})

Deno.test('Protocol.decode - throws SyntaxError for invalid JSON', () => {
  Assert.assertThrows(
    () => Protocol.decode(new TextEncoder().encode('{')),
    SyntaxError
  )
})

Deno.test('Protocol.encode - accepts a payload that fills the limit exactly', () => {
  const payload = { m: 'x' }
  const sized = Protocol.encode(Protocol.opcode.request, 1, payload)
  const length = sized.byteLength - 9
  const exact = Protocol.encode(Protocol.opcode.request, 1, payload, length)
  Assert.assertEquals(exact.byteLength, sized.byteLength)
  Assert.assertEquals(Protocol.decode(exact.subarray(9)), payload)
})

Deno.test('Protocol.encode - encodes null and omits undefined', () => {
  const absent = Protocol.encode(Protocol.opcode.success, 1)
  const empty = Protocol.encode(Protocol.opcode.success, 1, null)
  Assert.assertEquals(absent.byteLength, 9)
  Assert.assertEquals(Protocol.decode(empty.subarray(9)), null)
})

Deno.test('Protocol.encode - preserves unicode in the JSON body', () => {
  const payload = { text: 'cah\u00e9 \uD83D\uDCE6' }
  const frame = Protocol.encode(Protocol.opcode.request, 3, payload)
  Assert.assertEquals(Protocol.decode(frame.subarray(9)), payload)
})

Deno.test('Protocol.encode - rejects a payload one byte past the limit', () => {
  const payload = { m: 'x' }
  const sized = Protocol.encode(Protocol.opcode.request, 1, payload)
  Assert.assertThrows(
    () => Protocol.encode(Protocol.opcode.request, 1, payload, sized.byteLength - 9 - 1),
    RangeError,
    'exceeds maximum'
  )
})

Deno.test('Protocol.encode - returns an independent copy of a small frame', () => {
  const first = Protocol.encode(Protocol.opcode.request, 1, { n: 1 })
  const second = Protocol.encode(Protocol.opcode.request, 2, { n: 2 })
  Assert.assertEquals(Protocol.decode(first.subarray(9)), { n: 1 })
  Assert.assertEquals(Protocol.decode(second.subarray(9)), { n: 2 })
  Assert.assertEquals(first === second, false)
})

Deno.test('Protocol.encode - round-trips a JSON payload below the scratch cutoff', () => {
  const payload = { m: 'getUser', p: { id: 1, name: 'Budi' } }
  const frame = Protocol.encode(Protocol.opcode.request, 7, payload)
  Assert.assertEquals(frame[0], Protocol.opcode.request)
  Assert.assertEquals(new DataView(frame.buffer).getUint32(1), 7)
  Assert.assertEquals(new DataView(frame.buffer).getUint32(5), frame.byteLength - 9)
  Assert.assertEquals(Protocol.decode(frame.subarray(9)), payload)
})

Deno.test('Protocol.encode - round-trips a payload above the scratch cutoff', () => {
  const payload = { blob: 'x'.repeat(5000) }
  const frame = Protocol.encode(Protocol.opcode.success, 9, payload, Protocol.limit)
  Assert.assertEquals(Protocol.decode<typeof payload>(frame.subarray(9)), payload)
})

Deno.test('Protocol.encode - writes an empty body as a 9-byte header', () => {
  const frame = Protocol.encode(Protocol.opcode.ping, 0x01020304)
  Assert.assertEquals(frame.byteLength, 9)
  Assert.assertEquals(Array.from(frame), [0x04, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00])
})

Deno.test('Protocol.opcode - uses the documented frame bytes', () => {
  Assert.assertEquals(Protocol.opcode.request, 0x01)
  Assert.assertEquals(Protocol.opcode.success, 0x02)
  Assert.assertEquals(Protocol.opcode.failure, 0x03)
  Assert.assertEquals(Protocol.opcode.ping, 0x04)
  Assert.assertEquals(Protocol.opcode.pong, 0x05)
  Assert.assertEquals(Protocol.opcode.broadcast, 0x06)
})

Deno.test('Protocol.validateLimit - accepts the inclusive byte bounds', () => {
  Protocol.validateLimit(1)
  Protocol.validateLimit(Features.maximum)
  Protocol.validateLimit(Protocol.limit)
})

Deno.test('Protocol.validateLimit - rejects a fractional limit', () => {
  Assert.assertThrows(
    () => Protocol.validateLimit(1.5),
    RangeError,
    'Frame limit must be an integer between 1 and 4294967295'
  )
})

Deno.test('Protocol.validateLimit - rejects a limit above the 32-bit maximum', () => {
  Assert.assertThrows(
    () => Protocol.validateLimit(Features.maximum + 1),
    RangeError,
    'Frame limit must be an integer between 1 and 4294967295'
  )
})

Deno.test('Protocol.validateLimit - rejects a non-positive limit', () => {
  Assert.assertThrows(
    () => Protocol.validateLimit(0),
    RangeError,
    'Frame limit must be an integer between 1 and 4294967295'
  )
})
