import * as Assert from '@std/assert'
import WSRPC from '@neabyte/wsrpc'
import Client from '@app/client.ts'
import Server from '@app/server.ts'
import Features from '@tests/features.ts'

Deno.test('WSRPC - exposes Server as the class and Client as a connector', () => {
  Assert.assertEquals(WSRPC.Server, Server)
  Assert.assertEquals(typeof WSRPC.Client, 'function')
})

Deno.test('WSRPC.Client - rejects an empty hostname before dialing', async () => {
  await Assert.assertRejects(
    () => WSRPC.Client({ hostname: '   ', port: 1 }),
    TypeError,
    'Hostname must be a non-empty string'
  )
})

Deno.test('WSRPC.Client - returns a Client instance', async () => {
  const server = new Server().listen()
  const client = await WSRPC.Client({
    hostname: '127.0.0.1',
    port: server.addr.port
  })
  try {
    Assert.assertEquals(client instanceof Client, true)
  } finally {
    await Features.stop(client, server)
  }
})
