import type * as Types from '@app/types.ts'
import type Client from '@app/client.ts'
import WSRPC from '@neabyte/wsrpc'
import Reader from '@app/reader.ts'
import Server from '@app/server.ts'

export default class Features {
  static readonly maximum = 0xffffffff

  static connect(
    port: number,
    options: { timeoutMs?: number; reconnect?: number; frameLimit?: number } = {}
  ): Promise<Client> {
    const spec: Types.RpcClientOptions = { hostname: '127.0.0.1', port }
    if (options.timeoutMs !== undefined) {
      spec.timeoutMs = options.timeoutMs
    }
    if (options.reconnect !== undefined) {
      spec.reconnect = options.reconnect
    }
    if (options.frameLimit !== undefined) {
      spec.frameLimit = options.frameLimit
    }
    return WSRPC.Client(spec)
  }

  static dial(port: number): Promise<Deno.TcpConn> {
    return Deno.connect({ hostname: '127.0.0.1', port, transport: 'tcp' })
  }

  static gate(): { promise: Promise<void>; release: () => void } {
    let release: () => void = () => {}
    const promise = new Promise<void>((resolve) => {
      release = resolve
    })
    return { promise, release }
  }

  static header(opcode: number, ident: number, length: number): Uint8Array {
    const bytes = new Uint8Array(9)
    const view = new DataView(bytes.buffer)
    view.setUint8(0, opcode)
    view.setUint32(1, ident)
    view.setUint32(5, length)
    return bytes
  }

  static listen(port = 0): Deno.TcpListener {
    return Deno.listen({ hostname: '127.0.0.1', port, transport: 'tcp' })
  }

  static async pair(
    setup?: Types.RpcSetup,
    options: { timeoutMs?: number; reconnect?: number } = {}
  ): Promise<{ client: Client; server: Server }> {
    const server = new WSRPC.Server({ hostname: '127.0.0.1', port: 0 }, setup).listen()
    return { client: await Features.connect(server.addr.port, options), server }
  }

  static async peer(
    options: { timeoutMs?: number; reconnect?: number } = {}
  ): Promise<{ client: Client; listener: Deno.TcpListener; remote: Deno.TcpConn }> {
    const listener = Features.listen()
    const accepted = listener.accept()
    return {
      client: await Features.connect(listener.addr.port, options),
      listener,
      remote: await accepted
    }
  }

  static async raw(): Promise<{
    client: Deno.Conn
    reader: Reader
    server: Server
  }> {
    const server = Features.serve()
    const client = await Features.dial(server.addr.port)
    return { client, reader: new Reader(client), server }
  }

  static scripted(parts: (Uint8Array | null | 'zero')[], limit?: number): Deno.Conn {
    let index = 0
    return {
      read(buffer: Uint8Array): Promise<number | null> {
        const part = parts[index]
        if (part === undefined || part === null) {
          return Promise.resolve(null)
        }
        if (part === 'zero') {
          return Promise.resolve(0)
        }
        const count = Math.min(buffer.byteLength, part.byteLength, limit ?? buffer.byteLength)
        buffer.set(part.subarray(0, count))
        if (count === part.byteLength) {
          index += 1
        } else {
          parts[index] = part.subarray(count)
        }
        return Promise.resolve(count)
      }
    } as Deno.Conn
  }

  static async send(socket: Deno.Conn, frame: Uint8Array): Promise<void> {
    let offset = 0
    while (offset < frame.byteLength) {
      offset += await socket.write(frame.subarray(offset))
    }
  }

  static serve(setup?: Types.RpcSetup): Server {
    return new Server({ hostname: '127.0.0.1', port: 0 }, setup).listen()
  }

  static async stop(client: Client, server: Server): Promise<void> {
    client.close()
    server.close()
    await server.done
  }

  static writer(write: (chunk: Uint8Array) => Promise<number>): Deno.Conn {
    return { write } as Deno.Conn
  }
}
