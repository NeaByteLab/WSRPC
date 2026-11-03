import type * as Types from '@app/types.ts'
import Protocol from '@app/protocol.ts'
import Queue from '@app/queue.ts'
import Reader from '@app/reader.ts'

/**
 * Multiplexed TCP and WebSocket RPC server.
 * @description Accepts client connections, dispatches procedures, and broadcasts events.
 */
export default class Server {
  /** Active client connections and write queues */
  readonly #connections = new Map<Deno.Conn, Queue>()
  /** Maximum frame payload byte length */
  readonly #frameLimit: number
  /** Registered procedure handlers by method name */
  readonly #handlers = new Map<string, Types.RpcHandler<unknown, unknown>>()
  /** Underlying Deno TCP network listener */
  readonly #listener: Deno.TcpListener
  /** True when server is closed */
  #closed = false
  /** Active accept loop promise */
  #serving: Promise<void> | null = null

  /**
   * Create and bind RPC server.
   * @description Opens TCP listener and executes optional route setup callback.
   * @param options - Server bind and frame limit options
   * @param setup - Optional route registration callback
   * @throws {TypeError} When hostname or route handler is invalid
   * @throws {RangeError} When port or frameLimit is out of bounds
   */
  constructor(options: Types.RpcServerOptions = {}, setup?: Types.RpcSetup) {
    const hostname = options.hostname ?? '127.0.0.1'
    const targetPort = options.port ?? 0
    if (typeof hostname !== 'string' || hostname.trim().length === 0) {
      throw new TypeError('Hostname must be a non-empty string')
    }
    if (!Number.isSafeInteger(targetPort) || targetPort < 0 || targetPort > 65535) {
      throw new RangeError('Port must be an integer between 0 and 65535')
    }
    const frameLimit = options.frameLimit ?? Protocol.limit
    Protocol.validateLimit(frameLimit)
    this.#frameLimit = frameLimit
    this.#listener = Deno.listen({ hostname, port: targetPort, transport: 'tcp' })
    if (setup !== undefined) {
      try {
        setup(
          ((
            method: string,
            guards: readonly Types.RpcGuard[] | Types.RpcHandler,
            handler?: Types.RpcHandler
          ): void => {
            this.register(method, guards, handler)
          }) as Types.RpcRoute
        )
      } catch (caught) {
        try {
          this.#listener.close()
        } catch {
          void 0
        }
        throw caught
      }
    }
  }

  /** Bound network address of listener */
  get addr(): Deno.NetAddr {
    return this.#listener.addr as Deno.NetAddr
  }

  /** Promise resolving when server stops */
  get done(): Promise<void> {
    return this.#serving ?? Promise.resolve()
  }

  /**
   * Broadcast event to connected clients.
   * @description Encodes broadcast frame and enqueues write for all clients.
   * @param event - Broadcast event name to emit
   * @param payload - Optional event payload data
   * @returns Current server instance for chaining
   * @throws {TypeError} When event name is empty or invalid
   * @throws {RangeError} When encoded broadcast exceeds frame limit
   * @template TData - Broadcast payload value type
   */
  broadcast<TData = unknown>(event: string, payload?: TData): this {
    if (typeof event !== 'string' || event.length === 0) {
      throw new TypeError('Event name must be a non-empty string')
    }
    const frame = Protocol.encode(
      Protocol.opcode.broadcast,
      0,
      payload === undefined ? { e: event } : { e: event, d: payload },
      this.#frameLimit
    )
    if (this.#closed) {
      return this
    }
    for (const [socket, writer] of this.#connections) {
      writer.enqueue(socket, frame).catch(() => {})
    }
    return this
  }

  /** Close listener and all client sockets */
  close(): void {
    if (this.#closed) {
      return
    }
    this.#closed = true
    try {
      this.#listener.close()
    } catch {
      void 0
    }
    for (const socket of this.#connections.keys()) {
      try {
        socket.close()
      } catch {
        void 0
      }
    }
    this.#connections.clear()
  }

  /** Start accepting incoming client connections */
  listen(): this {
    if (this.#serving === null && !this.#closed) {
      this.#serving = (async (): Promise<void> => {
        try {
          for await (const socket of this.#listener) {
            if (this.#closed) {
              try {
                socket.close()
              } catch {
                void 0
              }
              break
            }
            Protocol.configure(socket)
            const writer = new Queue()
            this.#connections.set(socket, writer)
            void (async (): Promise<void> => {
              const reader = new Reader(socket, this.#frameLimit)
              try {
                while (!this.#closed) {
                  const frame = await reader.readFrame()
                  if (frame === null || frame.ident === 0) {
                    break
                  }
                  if (frame.opcode === Protocol.opcode.ping) {
                    if (frame.payload.byteLength > 0) {
                      break
                    }
                    await writer.enqueue(socket, Protocol.encode(Protocol.opcode.pong, frame.ident))
                    continue
                  }
                  if (frame.opcode !== Protocol.opcode.request) {
                    break
                  }
                  const ident = frame.ident
                  const payload = frame.payload
                  void (async (): Promise<void> => {
                    let response: Uint8Array | null = null
                    let request: Types.RpcRequest | null = null
                    if (payload.byteLength > 0) {
                      try {
                        const decoded = Protocol.decode<Types.RpcRequest>(payload)
                        if (
                          decoded !== null &&
                          typeof decoded === 'object' &&
                          !Array.isArray(decoded) &&
                          typeof decoded.m === 'string' &&
                          decoded.m.length > 0
                        ) {
                          request = decoded
                        }
                      } catch {
                        request = null
                      }
                    }
                    if (request === null) {
                      response = this.#failure(
                        ident,
                        'INVALID_REQUEST',
                        'Invalid RPC request envelope'
                      )
                    } else {
                      const handler = this.#handlers.get(request.m)
                      if (handler === undefined) {
                        response = this.#failure(
                          ident,
                          'METHOD_NOT_FOUND',
                          `Procedure ${request.m} is not registered`
                        )
                      } else {
                        try {
                          const result = await handler(request.p)
                          if (this.#closed || !this.#connections.has(socket)) {
                            return
                          }
                          response = Protocol.encode(
                            Protocol.opcode.success,
                            ident,
                            result,
                            this.#frameLimit
                          )
                        } catch (caught) {
                          if (this.#closed || !this.#connections.has(socket)) {
                            return
                          }
                          response = this.#failure(
                            ident,
                            'EXECUTION_ERROR',
                            caught instanceof Error ? caught.message : String(caught)
                          )
                        }
                      }
                    }
                    if (response === null) {
                      try {
                        socket.close()
                      } catch {
                        void 0
                      }
                      return
                    }
                    await writer.enqueue(socket, response).catch(() => {})
                  })()
                }
              } catch {
                void 0
              } finally {
                this.#connections.delete(socket)
                try {
                  socket.close()
                } catch {
                  void 0
                }
              }
            })()
          }
        } catch {
          void 0
        }
      })()
    }
    return this
  }

  /**
   * Register RPC procedure handler.
   * @description Binds method name to handler with optional middleware guards.
   * @param method - Procedure name to register
   * @param guards - Guard function array or direct procedure handler
   * @param handler - Procedure handler when guards array is provided
   * @returns Current server instance for chaining
   * @throws {TypeError} When method, guards, or handler is invalid
   * @template TParams - Procedure parameter value type
   * @template TResult - Procedure return value type
   */
  register<TParams = unknown, TResult = unknown>(
    method: string,
    guards: readonly Types.RpcGuard[] | Types.RpcHandler<TParams, TResult>,
    handler?: Types.RpcHandler<TParams, TResult>
  ): this {
    if (typeof method !== 'string' || method.length === 0) {
      throw new TypeError('Method name must be a non-empty string')
    }
    if (typeof guards === 'function') {
      this.#handlers.set(method, guards as Types.RpcHandler<unknown, unknown>)
      return this
    }
    if (!Array.isArray(guards)) {
      throw new TypeError('Middleware must be a function')
    }
    if (typeof handler !== 'function') {
      throw new TypeError('Handler must be a function')
    }
    for (const guard of guards) {
      if (typeof guard !== 'function') {
        throw new TypeError('Middleware must be a function')
      }
    }
    if (guards.length === 0) {
      this.#handlers.set(method, handler as Types.RpcHandler<unknown, unknown>)
      return this
    }
    this.#handlers.set(
      method,
      (async (params: TParams): Promise<TResult> => {
        let current: unknown = params
        for (const guard of guards) {
          current = await (guard as Types.RpcGuard<unknown>)(current)
        }
        return await handler(current as TParams)
      }) as Types.RpcHandler<unknown, unknown>
    )
    return this
  }

  /**
   * Encode failure response frame safely.
   * @description Falls back to status code message when payload overflows.
   * @param ident - Request sequence identifier
   * @param status - Error status code string
   * @param message - Error description message string
   * @returns Encoded failure frame or null if unencodable
   */
  #failure(ident: number, status: string, message: string): Uint8Array | null {
    try {
      return Protocol.encode(
        Protocol.opcode.failure,
        ident,
        { code: status, message },
        this.#frameLimit
      )
    } catch {
      try {
        return Protocol.encode(
          Protocol.opcode.failure,
          ident,
          { code: status, message: status },
          this.#frameLimit
        )
      } catch {
        return null
      }
    }
  }
}
