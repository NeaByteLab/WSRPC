import * as Assert from '@std/assert'
import Protocol from '@app/protocol.ts'
import Reader from '@app/reader.ts'
import Server from '@app/server.ts'
import Features from '@tests/features.ts'

Deno.test('Server - answers a call and a ping on one connection', async () => {
  const server = Features.serve((route) => {
    route('add', ({ a, b }: { a: number; b: number }) => a + b)
  })
  const client = await Features.dial(server.addr.port)
  const reader = new Reader(client)
  try {
    await Features.send(
      client,
      Protocol.encode(Protocol.opcode.request, 1, { m: 'add', p: { a: 2, b: 3 } })
    )
    await Features.send(client, Protocol.encode(Protocol.opcode.ping, 2))
    const result = await reader.readFrame()
    const probe = await reader.readFrame()
    if (result === null || probe === null) {
      throw new TypeError('expected both replies')
    }
    Assert.assertEquals(result.opcode, Protocol.opcode.success)
    Assert.assertEquals(Protocol.decode(result.payload), 5)
    Assert.assertEquals(probe.opcode, Protocol.opcode.pong)
    Assert.assertEquals(probe.ident, 2)
    Assert.assertEquals(probe.payload.byteLength, 0)
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - closes the socket after an invalid opcode', async () => {
  const { client, server } = await Features.raw()
  try {
    await Features.send(client, Features.header(0x07, 1, 0))
    Assert.assertEquals(await client.read(new Uint8Array(16)), null)
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - closes the socket when a request exceeds the frame limit', async () => {
  const server = new Server({ frameLimit: 16 }).listen()
  const client = await Features.dial(server.addr.port)
  try {
    await Features.send(client, Features.header(Protocol.opcode.request, 1, 17))
    Assert.assertEquals(await client.read(new Uint8Array(16)), null)
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - closes the socket when a request uses ident zero', async () => {
  const { client, server } = await Features.raw()
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 0, { m: 'x' }))
    Assert.assertEquals(await client.read(new Uint8Array(16)), null)
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - closes the socket when ping carries a payload', async () => {
  const { client, server } = await Features.raw()
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.ping, 1, { extra: true }))
    Assert.assertEquals(await client.read(new Uint8Array(16)), null)
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - does not answer a call that finishes after close', async () => {
  const gate = Features.gate()
  const server = Features.serve((route) => {
    route('hold', async () => {
      await gate.promise
      return 'late'
    })
  })
  const client = await Features.dial(server.addr.port)
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 1, { m: 'hold' }))
    await new Promise((resolve) => setTimeout(resolve, 20))
    server.close()
    gate.release()
    Assert.assertEquals(await client.read(new Uint8Array(16)), null)
  } finally {
    client.close()
    await server.done
  }
})

Deno.test('Server - drops a response that cannot fit the frame limit', async () => {
  const server = new Server({ frameLimit: 8 }).listen()
  server.register('wide', () => 'this payload cannot fit')
  const client = await Features.dial(server.addr.port)
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 1, { m: 'wide' }))
    Assert.assertEquals(await client.read(new Uint8Array(16)), null)
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - rejects a malformed request envelope', async () => {
  const { client, reader, server } = await Features.raw()
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 3, { m: '' }))
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 4, ['not-object']))
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 5, null))
    await Features.send(
      client,
      Protocol.encode(Protocol.opcode.request, 6, { m: 1 } as never)
    )
    for (const ident of [3, 4, 5, 6]) {
      const frame = await reader.readFrame()
      if (frame === null) {
        throw new TypeError('expected a failure')
      }
      Assert.assertEquals(frame.ident, ident)
      Assert.assertEquals(Protocol.decode(frame.payload), {
        code: 'INVALID_REQUEST',
        message: 'Invalid RPC request envelope'
      })
    }
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - replaces a method when it is registered again', async () => {
  const server = new Server()
  server.register('value', () => 1)
  server.register('value', () => 2)
  server.listen()
  const client = await Features.dial(server.addr.port)
  const reader = new Reader(client)
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 1, { m: 'value' }))
    const frame = await reader.readFrame()
    if (frame === null) {
      throw new TypeError('expected a result')
    }
    Assert.assertEquals(Protocol.decode(frame.payload), 2)
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - replies as each handler finishes, not in arrival order', async () => {
  const server = Features.serve((route) => {
    route('slow', async () => {
      await new Promise((resolve) => setTimeout(resolve, 40))
      return 'slow'
    })
    route('fast', () => 'fast')
  })
  const client = await Features.dial(server.addr.port)
  const reader = new Reader(client)
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 1, { m: 'slow' }))
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 2, { m: 'fast' }))
    const first = await reader.readFrame()
    const second = await reader.readFrame()
    if (first === null || second === null) {
      throw new TypeError('expected both replies')
    }
    Assert.assertEquals(first.ident, 2)
    Assert.assertEquals(Protocol.decode(first.payload), 'fast')
    Assert.assertEquals(second.ident, 1)
    Assert.assertEquals(Protocol.decode(second.payload), 'slow')
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - reports an unregistered method', async () => {
  const server = Features.serve()
  const client = await Features.dial(server.addr.port)
  const reader = new Reader(client)
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 9, { m: 'missing' }))
    const frame = await reader.readFrame()
    if (frame === null) {
      throw new TypeError('expected a failure')
    }
    Assert.assertEquals(Protocol.decode(frame.payload), {
      code: 'METHOD_NOT_FOUND',
      message: 'Procedure missing is not registered'
    })
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - runs guards before the handler', async () => {
  const server = Features.serve((route) => {
    route(
      'getUser',
      [(params: { id: number }) => {
        if (!Number.isSafeInteger(params.id) || params.id <= 0) {
          throw new TypeError('User id must be a positive integer')
        }
        return params
      }],
      ({ id }: { id: number }) => ({ id, name: 'Budi' })
    )
  })
  const client = await Features.dial(server.addr.port)
  const reader = new Reader(client)
  try {
    await Features.send(
      client,
      Protocol.encode(Protocol.opcode.request, 1, { m: 'getUser', p: { id: 1 } })
    )
    await Features.send(
      client,
      Protocol.encode(Protocol.opcode.request, 2, { m: 'getUser', p: { id: 0 } })
    )
    const success = await reader.readFrame()
    const failure = await reader.readFrame()
    if (success === null || failure === null) {
      throw new TypeError('expected both replies')
    }
    Assert.assertEquals(Protocol.decode(success.payload), { id: 1, name: 'Budi' })
    Assert.assertEquals(failure.opcode, Protocol.opcode.failure)
    Assert.assertEquals(Protocol.decode(failure.payload), {
      code: 'EXECUTION_ERROR',
      message: 'User id must be a positive integer'
    })
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - serves two clients at once', async () => {
  const server = Features.serve((route) => {
    route('echo', (value: string) => value)
  })
  const first = await Features.dial(server.addr.port)
  const second = await Features.dial(server.addr.port)
  const firstReader = new Reader(first)
  const secondReader = new Reader(second)
  try {
    await Features.send(
      first,
      Protocol.encode(Protocol.opcode.request, 1, { m: 'echo', p: 'L' })
    )
    await Features.send(
      second,
      Protocol.encode(Protocol.opcode.request, 2, { m: 'echo', p: 'R' })
    )
    const firstFrame = await firstReader.readFrame()
    const secondFrame = await secondReader.readFrame()
    if (firstFrame === null || secondFrame === null) {
      throw new TypeError('expected both replies')
    }
    Assert.assertEquals(Protocol.decode(firstFrame.payload), 'L')
    Assert.assertEquals(Protocol.decode(secondFrame.payload), 'R')
  } finally {
    first.close()
    second.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - stringifies a thrown non-Error value', async () => {
  const server = Features.serve((route) => {
    route('boom', () => {
      throw 'plain'
    })
  })
  const client = await Features.dial(server.addr.port)
  const reader = new Reader(client)
  try {
    await Features.send(client, Protocol.encode(Protocol.opcode.request, 1, { m: 'boom' }))
    const frame = await reader.readFrame()
    if (frame === null) {
      throw new TypeError('expected a failure')
    }
    Assert.assertEquals(Protocol.decode(frame.payload), {
      code: 'EXECUTION_ERROR',
      message: 'plain'
    })
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server - treats invalid JSON as an invalid request', async () => {
  const payload = new TextEncoder().encode('{')
  const frame = new Uint8Array(9 + payload.byteLength)
  frame.set(Features.header(Protocol.opcode.request, 1, payload.byteLength), 0)
  frame.set(payload, 9)
  const { client, reader, server } = await Features.raw()
  try {
    await Features.send(client, frame)
    const reply = await reader.readFrame()
    if (reply === null) {
      throw new TypeError('expected a failure')
    }
    Assert.assertEquals(Protocol.decode(reply.payload), {
      code: 'INVALID_REQUEST',
      message: 'Invalid RPC request envelope'
    })
  } finally {
    client.close()
    server.close()
    await server.done
  }
})

Deno.test('Server constructor - binds an ephemeral loopback port by default', () => {
  const server = new Server()
  try {
    Assert.assertEquals(server.addr.hostname, '127.0.0.1')
    Assert.assertEquals(server.addr.port > 0, true)
  } finally {
    server.close()
  }
})

Deno.test('Server constructor - closes the listener when setup throws', () => {
  Assert.assertThrows(
    () =>
      new Server({}, () => {
        throw new TypeError('setup failed')
      }),
    TypeError,
    'setup failed'
  )
})

Deno.test('Server constructor - rejects a blank hostname', () => {
  Assert.assertThrows(
    () => new Server({ hostname: '   ' }),
    TypeError,
    'Hostname must be a non-empty string'
  )
})

Deno.test('Server constructor - rejects a port outside 0 through 65535', () => {
  Assert.assertThrows(
    () => new Server({ port: 65536 }),
    RangeError,
    'Port must be an integer between 0 and 65535'
  )
})

Deno.test('Server constructor - rejects an invalid frame limit', () => {
  Assert.assertThrows(
    () => new Server({ frameLimit: 0 }),
    RangeError,
    'Frame limit must be an integer between 1 and 4294967295'
  )
})

Deno.test('Server.broadcast - rejects an empty event name', () => {
  const server = new Server()
  try {
    Assert.assertThrows(
      () => server.broadcast(''),
      TypeError,
      'Event name must be a non-empty string'
    )
  } finally {
    server.close()
  }
})

Deno.test('Server.broadcast - sends an event to all connected clients', async () => {
  const server = Features.serve()
  const first = await Features.connect(server.addr.port)
  const second = await Features.connect(server.addr.port)
  const firstGate = Features.gate()
  const secondGate = Features.gate()
  let firstReceived: { id: number; name: string } | null = null
  let secondReceived: { id: number; name: string } | null = null
  first.on('userUpdated', (data: { id: number; name: string }) => {
    firstReceived = data
    firstGate.release()
  })
  second.on('userUpdated', (data: { id: number; name: string }) => {
    secondReceived = data
    secondGate.release()
  })
  try {
    await first.ping()
    await second.ping()
    Assert.assertEquals(server.broadcast('userUpdated', { id: 1, name: 'Nea' }), server)
    await Promise.all([firstGate.promise, secondGate.promise])
    Assert.assertEquals(firstReceived, { id: 1, name: 'Nea' })
    Assert.assertEquals(secondReceived, { id: 1, name: 'Nea' })
  } finally {
    first.close()
    second.close()
    server.close()
    await server.done
  }
})

Deno.test('Server.close - is idempotent before listen', async () => {
  const server = new Server()
  server.close()
  server.close()
  await server.done
})

Deno.test('Server.listen - starts the accept loop once', () => {
  const server = new Server()
  try {
    Assert.assertEquals(server.listen(), server)
    const first = server.done
    Assert.assertEquals(server.listen().done, first)
  } finally {
    server.close()
  }
})

Deno.test('Server.register - rejects a missing handler', () => {
  const server = new Server()
  try {
    Assert.assertThrows(
      () => server.register('echo', [] as never),
      TypeError,
      'Handler must be a function'
    )
  } finally {
    server.close()
  }
})

Deno.test('Server.register - rejects a non-function guard', () => {
  const server = new Server()
  try {
    Assert.assertThrows(
      () => server.register('echo', [1 as never], () => null),
      TypeError,
      'Middleware must be a function'
    )
  } finally {
    server.close()
  }
})

Deno.test('Server.register - rejects an empty method name', () => {
  const server = new Server()
  try {
    Assert.assertThrows(
      () => server.register('', () => null),
      TypeError,
      'Method name must be a non-empty string'
    )
  } finally {
    server.close()
  }
})
