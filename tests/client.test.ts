import * as Assert from '@std/assert'
import WSRPC from '@neabyte/wsrpc'
import Protocol from '@app/protocol.ts'
import Reader from '@app/reader.ts'
import Features from '@tests/features.ts'

Deno.test('Client - closes when the server sends a request opcode', async () => {
  const { client, listener, remote } = await Features.peer()
  try {
    await remote.write(Protocol.encode(Protocol.opcode.request, 1, { m: 'nope' }))
    await new Promise((resolve) => setTimeout(resolve, 50))
    await Assert.assertRejects(() => client.ping(), TypeError, 'RPC client connection is closed')
  } finally {
    client.close()
    remote.close()
    listener.close()
  }
})

Deno.test('Client - ignores a response for an unknown identifier', async () => {
  const { client, server } = await Features.pair((route) => {
    route('ok', () => true)
  })
  const socket = await Features.dial(server.addr.port)
  try {
    await socket.write(Protocol.encode(Protocol.opcode.success, 99, true))
    Assert.assertEquals(await client.call('ok'), true)
  } finally {
    socket.close()
    await Features.stop(client, server)
  }
})

Deno.test('Client - reconnects after the server comes back', async () => {
  const first = new WSRPC.Server({ hostname: '127.0.0.1', port: 0 }, (route) => {
    route('name', () => 'first')
  }).listen()
  const address = first.addr.port
  const client = await Features.connect(address, { reconnect: 4, timeoutMs: 1000 })
  try {
    Assert.assertEquals(await client.call('name'), 'first')
    first.close()
    await first.done
    await new Promise((resolve) => setTimeout(resolve, 100))
    const second = new WSRPC.Server({ hostname: '127.0.0.1', port: address }, (route) => {
      route('name', () => 'second')
    }).listen()
    try {
      await new Promise((resolve) => setTimeout(resolve, 800))
      Assert.assertEquals(await client.call('name'), 'second')
    } finally {
      second.close()
      await second.done
    }
  } finally {
    client.close()
  }
})

Deno.test('Client - rejects a failure envelope without a message', async () => {
  const { client, listener, remote } = await Features.peer()
  const reader = new Reader(remote)
  try {
    const pending = client.call('ghost')
    const request = await reader.readFrame()
    if (request === null) {
      throw new TypeError('expected a request')
    }
    await remote.write(Protocol.encode(Protocol.opcode.failure, request.ident, { code: 'BROKEN' }))
    await Assert.assertRejects(() => pending, TypeError, 'Invalid RPC failure envelope')
  } finally {
    client.close()
    remote.close()
    listener.close()
  }
})

Deno.test('Client - rejects a success payload that is not JSON', async () => {
  const { client, listener, remote } = await Features.peer()
  const reader = new Reader(remote)
  try {
    const pending = client.call('ghost')
    const request = await reader.readFrame()
    if (request === null) {
      throw new TypeError('expected a request')
    }
    const payload = new TextEncoder().encode('{')
    const frame = new Uint8Array(9 + payload.byteLength)
    const header = new DataView(frame.buffer)
    header.setUint8(0, Protocol.opcode.success)
    header.setUint32(1, request.ident)
    header.setUint32(5, payload.byteLength)
    frame.set(payload, 9)
    await remote.write(frame)
    await Assert.assertRejects(() => pending, SyntaxError)
    await Assert.assertRejects(
      () => client.call('after'),
      TypeError,
      'RPC client connection is closed'
    )
  } finally {
    client.close()
    remote.close()
    listener.close()
  }
})

Deno.test('Client - reports a failure code alone when the message is empty', async () => {
  const { client, listener, remote } = await Features.peer()
  const reader = new Reader(remote)
  try {
    const pending = client.call('ghost')
    const request = await reader.readFrame()
    if (request === null) {
      throw new TypeError('expected a request')
    }
    await remote.write(
      Protocol.encode(Protocol.opcode.failure, request.ident, { code: 'ONLY_CODE', message: '' })
    )
    await Assert.assertRejects(() => pending, TypeError, 'ONLY_CODE')
  } finally {
    client.close()
    remote.close()
    listener.close()
  }
})

Deno.test('Client - retries an in-flight call after the socket is restored', async () => {
  const { client, listener: first, remote } = await Features.peer({ reconnect: 2, timeoutMs: 3000 })
  const address = first.addr.port
  const pending = client.call('name')
  const reader = new Reader(remote)
  const request = await reader.readFrame()
  if (request === null) {
    throw new TypeError('expected the first request')
  }
  remote.close()
  first.close()
  await new Promise((resolve) => setTimeout(resolve, 200))
  const second = Features.listen(address)
  try {
    const again = await second.accept()
    const againReader = new Reader(again)
    const retried = await againReader.readFrame()
    if (retried === null) {
      throw new TypeError('expected the retried request')
    }
    Assert.assertEquals(retried.ident, request.ident)
    await again.write(Protocol.encode(Protocol.opcode.success, retried.ident, 'second'))
    Assert.assertEquals(await pending, 'second')
    again.close()
  } finally {
    client.close()
    second.close()
  }
})

Deno.test('Client - shuts down after the reconnect budget is spent', async () => {
  const server = new WSRPC.Server().listen()
  const client = await Features.connect(server.addr.port, { reconnect: 1, timeoutMs: 1000 })
  server.close()
  await server.done
  await new Promise((resolve) => setTimeout(resolve, 1200))
  try {
    await Assert.assertRejects(() => client.ping(), TypeError, 'RPC client connection is closed')
  } finally {
    client.close()
  }
})

Deno.test('Client - stops reconnecting when closed during the delay', async () => {
  const server = new WSRPC.Server().listen()
  const client = await Features.connect(server.addr.port, { reconnect: 5, timeoutMs: 1000 })
  server.close()
  await server.done
  await new Promise((resolve) => setTimeout(resolve, 50))
  client.close()
  await Assert.assertRejects(
    () => client.call('late'),
    TypeError,
    'RPC client connection is closed'
  )
})

Deno.test('Client.call - accepts an undefined success payload', async () => {
  const { client, listener, remote } = await Features.peer()
  const reader = new Reader(remote)
  try {
    const pending = client.call('echo')
    const request = await reader.readFrame()
    if (request === null) {
      throw new TypeError('expected a request')
    }
    await remote.write(Protocol.encode(Protocol.opcode.success, request.ident))
    Assert.assertEquals(await pending, undefined)
  } finally {
    client.close()
    remote.close()
    listener.close()
  }
})

Deno.test('Client.call - keeps two in-flight calls distinct', async () => {
  const { client, server } = await Features.pair((route) => {
    route('id', (value: number) => value)
  })
  try {
    const [first, second] = await Promise.all([client.call('id', 1), client.call('id', 2)])
    Assert.assertEquals(first, 1)
    Assert.assertEquals(second, 2)
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.call - omits params when none are given', async () => {
  const { client, server } = await Features.pair((route) => {
    route('pingless', (params: unknown) => params === undefined)
  })
  try {
    Assert.assertEquals(await client.call('pingless'), true)
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.call - rejects a payload that exceeds the frame limit', async () => {
  const server = new WSRPC.Server({ frameLimit: 8 }).listen()
  const client = await Features.connect(server.addr.port, { frameLimit: 8, timeoutMs: 1000 })
  try {
    await Assert.assertRejects(
      () => client.call('echo', { blob: 'too-large' }),
      RangeError,
      'exceeds maximum'
    )
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.call - rejects after the client is closed', async () => {
  const { client, server } = await Features.pair()
  client.close()
  try {
    await Assert.assertRejects(
      () => client.call('echo'),
      TypeError,
      'RPC client connection is closed'
    )
  } finally {
    server.close()
    await server.done
  }
})

Deno.test('Client.call - rejects an empty method name', async () => {
  const { client, server } = await Features.pair()
  try {
    await Assert.assertRejects(
      () => client.call(''),
      TypeError,
      'Method name must be a non-empty string'
    )
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.call - rejects an invalid per-call timeout', async () => {
  const { client, server } = await Features.pair()
  try {
    await Assert.assertRejects(
      () => client.call('echo', null, Number.NaN),
      RangeError,
      'Timeout must be a positive finite number'
    )
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.call - rejects pending calls when the socket drops', async () => {
  const { client, server } = await Features.pair((route) => {
    route('hold', () => new Promise(() => {}))
  })
  const pending = client.call('hold', undefined, 2000)
  server.close()
  try {
    const error = await Assert.assertRejects(() => pending)
    Assert.assertEquals(error instanceof Error, true)
  } finally {
    client.close()
    await server.done
  }
})

Deno.test('Client.call - reports a thrown handler error', async () => {
  const { client, server } = await Features.pair((route) => {
    route('fail', () => {
      throw new TypeError('bad input')
    })
  })
  try {
    await Assert.assertRejects(() => client.call('fail'), TypeError, 'EXECUTION_ERROR bad input')
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.call - reports an unregistered method', async () => {
  const { client, server } = await Features.pair()
  try {
    await Assert.assertRejects(
      () => client.call('missing'),
      TypeError,
      'METHOD_NOT_FOUND Procedure missing is not registered'
    )
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.call - returns the handler result', async () => {
  const { client, server } = await Features.pair((route) => {
    route(
      'getUser',
      [
        ({ id }: { id: number }) => {
          if (!Number.isSafeInteger(id) || id <= 0) {
            throw new TypeError('User id must be a positive integer')
          }
          return { id }
        }
      ],
      ({ id }: { id: number }) => ({ id, name: 'Budi' })
    )
  })
  try {
    Assert.assertEquals(await client.call('getUser', { id: 1 }), { id: 1, name: 'Budi' })
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.call - times out when the peer never replies', async () => {
  const { client, listener, remote } = await Features.peer({ timeoutMs: 30 })
  try {
    await Assert.assertRejects(
      () => client.call('missing', undefined, 30),
      RangeError,
      'RPC timeout after 30ms calling missing'
    )
  } finally {
    client.close()
    remote.close()
    listener.close()
  }
})

Deno.test('Client.close - rejects a call that is still pending', async () => {
  const { client, server } = await Features.pair((route) => {
    route('hold', () => new Promise(() => {}))
  })
  const pending = client.call('hold', undefined, 2000)
  client.close()
  try {
    await Assert.assertRejects(() => pending, TypeError, 'RPC client closed')
  } finally {
    server.close()
    await server.done
  }
})

Deno.test('Client.connect - rejects a blank hostname', async () => {
  await Assert.assertRejects(
    () => WSRPC.Client({ hostname: ' ', port: 1 }),
    TypeError,
    'Hostname must be a non-empty string'
  )
})

Deno.test('Client.connect - rejects a negative reconnect count', async () => {
  await Assert.assertRejects(
    () => WSRPC.Client({ hostname: '127.0.0.1', port: 1, reconnect: -1 }),
    RangeError,
    'Reconnect count must be a non-negative integer'
  )
})

Deno.test('Client.connect - rejects a non-positive timeout', async () => {
  await Assert.assertRejects(
    () => WSRPC.Client({ hostname: '127.0.0.1', port: 1, timeoutMs: 0 }),
    RangeError,
    'Timeout must be a positive finite number'
  )
})

Deno.test('Client.connect - rejects a port outside 1 through 65535', async () => {
  await Assert.assertRejects(
    () => WSRPC.Client({ hostname: '127.0.0.1', port: 0 }),
    RangeError,
    'Port must be an integer between 1 and 65535'
  )
})

Deno.test('Client.connect - rejects a timeout above the 32-bit signed maximum', async () => {
  await Assert.assertRejects(
    () => WSRPC.Client({ hostname: '127.0.0.1', port: 1, timeoutMs: 0x80000000 }),
    RangeError,
    'Timeout must be a positive finite number'
  )
})

Deno.test('Client.connect - rejects an invalid frame limit', async () => {
  await Assert.assertRejects(
    () => WSRPC.Client({ hostname: '127.0.0.1', port: 1, frameLimit: 0 }),
    RangeError,
    'Frame limit must be an integer between 1 and 4294967295'
  )
})

Deno.test('Client.on - receives broadcast events without data', async () => {
  const { client, server } = await Features.pair()
  const gate = Features.gate()
  let received: unknown = 'initial'
  client.on('pinged', (data) => {
    received = data
    gate.release()
  })
  try {
    await client.ping()
    server.broadcast('pinged')
    await gate.promise
    Assert.assertEquals(received, undefined)
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.on - rejects an empty event name or non-function listener', async () => {
  const { client, server } = await Features.pair()
  try {
    Assert.assertThrows(
      () => client.on('', () => {}),
      TypeError,
      'Event name must be a non-empty string'
    )
    Assert.assertThrows(
      () => client.on('tick', 1 as never),
      TypeError,
      'Listener must be a function'
    )
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.ping - rejects after the client is closed', async () => {
  const { client, server } = await Features.pair()
  client.close()
  try {
    await Assert.assertRejects(() => client.ping(), TypeError, 'RPC client connection is closed')
  } finally {
    server.close()
    await server.done
  }
})

Deno.test('Client.ping - rejects an invalid timeout', async () => {
  const { client, server } = await Features.pair()
  try {
    await Assert.assertRejects(
      () => client.ping(0),
      RangeError,
      'Timeout must be a positive finite number'
    )
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.ping - resolves true when the server answers pong', async () => {
  const { client, server } = await Features.pair()
  try {
    Assert.assertEquals(await client.ping(), true)
  } finally {
    await Features.stop(client, server)
  }
})

Deno.test('Client.ping - times out when pong never arrives', async () => {
  const { client, listener, remote } = await Features.peer({ timeoutMs: 40 })
  try {
    await Assert.assertRejects(() => client.ping(40), RangeError, 'RPC ping timeout after 40ms')
  } finally {
    client.close()
    remote.close()
    listener.close()
  }
})
