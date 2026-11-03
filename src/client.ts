import type * as Types from '@app/types.ts'
import Protocol from '@app/protocol.ts'
import Queue from '@app/queue.ts'
import Reader from '@app/reader.ts'

/**
 * Multiplexed TCP and WebSocket RPC client.
 * @description Manages connection lifecycle, procedure calls, pings, and broadcasts.
 */
export default class Client {
  /** Registered broadcast event listeners */
  readonly #listeners = new Map<string, Types.RpcListener<unknown>>()
  /** Active request tickets by identifier */
  readonly #pending = new Map<number, Types.RpcTicket>()
  /** Outbound frame write queue */
  readonly #writer = new Queue()
  /** Reconnect delay cancellation callback */
  #cancelDelay: (() => void) | null = null
  /** True when client is closed */
  #closed = false
  /** Maximum frame payload byte length */
  #frameLimit = 0
  /** Target server hostname or IP */
  #hostname = ''
  /** Maximum reconnection attempts allowed */
  #reconnect = 0
  /** Active reconnection task promise */
  #restoring: Promise<boolean> | null = null
  /** Monotonic frame sequence counter */
  #sequence = 0
  /** Active underlying transport connection */
  #socket!: Deno.TcpConn
  /** Target server TCP port */
  #targetPort = 0
  /** Default request timeout in milliseconds */
  #timeout = 0

  /**
   * Initialize connected RPC client instance.
   * @description Starts background frame reader loop and reconnection handler.
   * @param socket - Connected transport socket instance
   * @param hostname - Target server hostname or IP
   * @param targetPort - Target server port number
   * @param timeout - Default request timeout in milliseconds
   * @param frameLimit - Maximum allowed frame payload byte length
   * @param reconnect - Maximum reconnection attempts on disconnect
   */
  private constructor(
    socket: Deno.TcpConn,
    hostname: string,
    targetPort: number,
    timeout: number,
    frameLimit: number,
    reconnect: number
  ) {
    this.#socket = socket
    this.#hostname = hostname
    this.#targetPort = targetPort
    this.#timeout = timeout
    this.#frameLimit = frameLimit
    this.#reconnect = reconnect
    Protocol.configure(this.#socket)
    void (async (): Promise<void> => {
      while (!this.#closed) {
        const reader = new Reader(this.#socket, this.#frameLimit)
        let failure: Error = new TypeError('RPC TCP stream closed by remote peer')
        try {
          while (!this.#closed) {
            const frame = await reader.readFrame()
            if (frame === null) {
              break
            }
            if (frame.opcode === Protocol.opcode.broadcast) {
              if (frame.ident !== 0 || frame.payload.byteLength === 0) {
                throw new TypeError(
                  `Invalid frame received by RPC client with opcode ${frame.opcode}`
                )
              }
              const decoded = Protocol.decode<Types.RpcBroadcast>(frame.payload)
              if (
                decoded === null ||
                typeof decoded !== 'object' ||
                Array.isArray(decoded) ||
                typeof decoded.e !== 'string' ||
                decoded.e.length === 0
              ) {
                throw new TypeError('Invalid RPC broadcast envelope')
              }
              const listener = this.#listeners.get(decoded.e)
              if (listener !== undefined) {
                Promise.resolve()
                  .then(() => listener(decoded.d))
                  .catch(() => {})
              }
              continue
            }
            if (
              frame.ident === 0 ||
              frame.opcode === Protocol.opcode.request ||
              frame.opcode === Protocol.opcode.ping ||
              (frame.opcode === Protocol.opcode.pong && frame.payload.byteLength > 0) ||
              (frame.opcode === Protocol.opcode.failure && frame.payload.byteLength === 0)
            ) {
              throw new TypeError(
                `Invalid frame received by RPC client with opcode ${frame.opcode}`
              )
            }
            const pending = this.#pending.get(frame.ident)
            if (pending === undefined) {
              continue
            }
            clearTimeout(pending.timer)
            this.#pending.delete(frame.ident)
            try {
              if (pending.isPing) {
                if (frame.opcode !== Protocol.opcode.pong) {
                  throw new TypeError(`Unexpected frame opcode ${frame.opcode} for ping request`)
                }
                pending.resolve(true)
                continue
              }
              if (frame.opcode === Protocol.opcode.success) {
                pending.resolve(
                  frame.payload.byteLength > 0 ? Protocol.decode<unknown>(frame.payload) : undefined
                )
                continue
              }
              if (frame.opcode !== Protocol.opcode.failure) {
                throw new TypeError(`Unexpected frame opcode ${frame.opcode} for RPC response`)
              }
              const decoded = Protocol.decode<Types.RpcFailure>(frame.payload)
              if (
                decoded === null ||
                typeof decoded !== 'object' ||
                Array.isArray(decoded) ||
                typeof decoded.code !== 'string' ||
                decoded.code.length === 0 ||
                typeof decoded.message !== 'string'
              ) {
                throw new TypeError('Invalid RPC failure envelope')
              }
              pending.reject(
                new TypeError(
                  decoded.message.length > 0 ? `${decoded.code} ${decoded.message}` : decoded.code
                )
              )
            } catch (caught) {
              pending.reject(caught as Error)
              throw caught
            }
          }
        } catch (caught) {
          failure = caught instanceof Error ? caught : new TypeError(String(caught))
        }
        if (this.#closed) {
          return
        }
        if (this.#reconnect === 0) {
          this.#shutdown(failure)
          return
        }
        try {
          this.#socket.close()
        } catch {
          void 0
        }
        this.#writer.reset()
        this.#restoring = (async (): Promise<boolean> => {
          for (let attempt = 1; attempt <= this.#reconnect && !this.#closed; attempt += 1) {
            await new Promise<void>((resolve) => {
              const timer = setTimeout(() => {
                this.#cancelDelay = null
                resolve()
              }, 500)
              this.#cancelDelay = () => {
                clearTimeout(timer)
                this.#cancelDelay = null
                resolve()
              }
            })
            if (this.#closed) {
              return false
            }
            try {
              const socket = await Client.#openSocket(this.#hostname, this.#targetPort)
              if (this.#closed) {
                try {
                  socket.close()
                } catch {
                  void 0
                }
                return false
              }
              this.#socket = socket
              Protocol.configure(this.#socket)
              return true
            } catch {
              void 0
            }
          }
          return false
        })()
        if (!(await this.#restoring)) {
          this.#restoring = null
          this.#shutdown(failure)
          return
        }
        this.#restoring = null
        for (const [ident, pending] of this.#pending) {
          this.#write(ident, pending.frame, pending.reject, pending.timer)
        }
      }
    })()
  }

  /**
   * Invoke remote RPC procedure.
   * @description Encodes request frame and awaits matching server response.
   * @param method - Remote procedure name to invoke
   * @param params - Optional parameters passed to procedure
   * @param timeoutMs - Custom call timeout in milliseconds
   * @returns Promise resolving with decoded procedure result
   * @template TResult - Expected return value type
   * @template TParams - Request parameter value type
   */
  call<TResult = unknown, TParams = unknown>(
    method: string,
    params?: TParams,
    timeoutMs: number = this.#timeout
  ): Promise<TResult> {
    if (this.#closed) {
      return Promise.reject(new TypeError('RPC client connection is closed'))
    }
    if (typeof method !== 'string' || method.length === 0) {
      return Promise.reject(new TypeError('Method name must be a non-empty string'))
    }
    try {
      Client.#checkTimeout(timeoutMs)
    } catch (caught) {
      return Promise.reject(caught as RangeError)
    }
    const ident = this.#nextIdent()
    let frame: Uint8Array
    try {
      frame = Protocol.encode(
        Protocol.opcode.request,
        ident,
        params === undefined ? { m: method } : { m: method, p: params },
        this.#frameLimit
      )
    } catch (caught) {
      return Promise.reject(caught instanceof Error ? caught : new TypeError(String(caught)))
    }
    return new Promise<TResult>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(ident)
        reject(new RangeError(`RPC timeout after ${timeoutMs}ms calling ${method}`))
      }, timeoutMs)
      this.#pending.set(ident, {
        frame,
        resolve: resolve as (value: unknown) => void,
        reject,
        timer,
        isPing: false
      })
      if (this.#restoring === null) {
        this.#write(ident, frame, reject, timer)
      }
    })
  }

  /** Close client connection immediately */
  close(): void {
    this.#shutdown(new TypeError('RPC client closed'))
  }

  /**
   * Connect to remote RPC server.
   * @description Validates options, opens transport socket, and creates client.
   * @param options - Client connection configuration options
   * @returns Promise resolving with connected Client instance
   * @throws {TypeError} When hostname is invalid or connection fails
   * @throws {RangeError} When port, timeout, frameLimit, or reconnect is invalid
   */
  static async connect(options: Types.RpcClientOptions): Promise<Client> {
    if (typeof options.hostname !== 'string' || options.hostname.trim().length === 0) {
      throw new TypeError('Hostname must be a non-empty string')
    }
    if (!Number.isSafeInteger(options.port) || options.port < 1 || options.port > 65535) {
      throw new RangeError('Port must be an integer between 1 and 65535')
    }
    const timeoutMs = options.timeoutMs ?? 5000
    const frameLimit = options.frameLimit ?? Protocol.limit
    const reconnect = options.reconnect ?? 0
    Client.#checkTimeout(timeoutMs)
    Protocol.validateLimit(frameLimit)
    if (!Number.isSafeInteger(reconnect) || reconnect < 0) {
      throw new RangeError('Reconnect count must be a non-negative integer')
    }
    return new Client(
      await Client.#openSocket(options.hostname, options.port),
      options.hostname,
      options.port,
      timeoutMs,
      frameLimit,
      reconnect
    )
  }

  /**
   * Subscribe to server broadcast event.
   * @description Registers listener callback for matching broadcast event name.
   * @param event - Broadcast event name to listen for
   * @param listener - Callback invoked with decoded event payload
   * @returns Current client instance for chaining
   * @throws {TypeError} When event name or listener is invalid
   * @template TData - Expected broadcast payload type
   */
  on<TData = unknown>(event: string, listener: Types.RpcListener<TData>): this {
    if (typeof event !== 'string' || event.length === 0) {
      throw new TypeError('Event name must be a non-empty string')
    }
    if (typeof listener !== 'function') {
      throw new TypeError('Listener must be a function')
    }
    this.#listeners.set(event, listener as Types.RpcListener<unknown>)
    return this
  }

  /**
   * Send keepalive ping to server.
   * @description Transmits ping frame and awaits matching pong frame.
   * @param timeoutMs - Custom ping timeout in milliseconds
   * @returns Promise resolving to true on pong response
   */
  ping(timeoutMs: number = this.#timeout): Promise<boolean> {
    if (this.#closed) {
      return Promise.reject(new TypeError('RPC client connection is closed'))
    }
    try {
      Client.#checkTimeout(timeoutMs)
    } catch (caught) {
      return Promise.reject(caught as RangeError)
    }
    const ident = this.#nextIdent()
    const frame = Protocol.encode(Protocol.opcode.ping, ident, undefined, this.#frameLimit)
    return new Promise<boolean>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(ident)
        reject(new RangeError(`RPC ping timeout after ${timeoutMs}ms`))
      }, timeoutMs)
      this.#pending.set(ident, {
        frame,
        resolve: resolve as (value: unknown) => void,
        reject,
        timer,
        isPing: true
      })
      if (this.#restoring === null) {
        this.#write(ident, frame, reject, timer)
      }
    })
  }

  /**
   * Validate timeout duration in milliseconds.
   * @description Ensures timeout is a bounded positive finite number.
   * @param timeoutMs - Timeout value in milliseconds
   * @throws {RangeError} When timeout is not a valid positive number
   */
  static #checkTimeout(timeoutMs: number): void {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > 0x7fffffff) {
      throw new RangeError('Timeout must be a positive finite number')
    }
  }

  /**
   * Open raw TCP or WebSocket connection.
   * @description Uses Deno TCP when available or falls back to WebSocket.
   * @param hostname - Target server hostname or IP
   * @param targetPort - Target server TCP port
   * @returns Promise resolving to connected transport socket
   */
  static #openSocket(hostname: string, targetPort: number): Promise<Deno.TcpConn> {
    if (typeof globalThis.Deno?.connect === 'function') {
      return Deno.connect({ hostname, port: targetPort, transport: 'tcp' })
    }
    return new Promise<Deno.TcpConn>((resolve, reject) => {
      const channel = new WebSocket(
        `ws://${
          hostname.includes(':') && !hostname.startsWith('[') ? `[${hostname}]` : hostname
        }:${targetPort}`
      )
      channel.binaryType = 'arraybuffer'
      const chunks: Uint8Array[] = []
      let waiter: ((value: number | null) => void) | null = null
      let target: Uint8Array | null = null
      let closed = false
      let opened = false
      channel.onopen = () => {
        opened = true
        resolve({
          read(buffer: Uint8Array): Promise<number | null> {
            if (chunks.length > 0) {
              const first = chunks[0] as Uint8Array
              const count = Math.min(buffer.byteLength, first.byteLength)
              buffer.set(first.subarray(0, count), 0)
              if (count === first.byteLength) {
                chunks.shift()
              } else {
                chunks[0] = first.subarray(count)
              }
              return Promise.resolve(count)
            }
            if (closed) {
              return Promise.resolve(null)
            }
            return new Promise<number | null>((resume) => {
              target = buffer
              waiter = resume
            })
          },
          write(frame: Uint8Array): Promise<number> {
            if (closed || channel.readyState !== WebSocket.OPEN) {
              return Promise.resolve(0)
            }
            channel.send(frame)
            return Promise.resolve(frame.byteLength)
          },
          close(): void {
            closed = true
            chunks.length = 0
            try {
              channel.close()
            } catch {
              void 0
            }
            if (waiter !== null) {
              const resume = waiter
              waiter = null
              target = null
              resume(null)
            }
          }
        } as unknown as Deno.TcpConn)
      }
      channel.onmessage = (message: MessageEvent) => {
        const incoming = new Uint8Array(message.data as ArrayBuffer)
        if (incoming.byteLength === 0) {
          return
        }
        if (waiter !== null && target !== null) {
          const resume = waiter
          const buffer = target
          waiter = null
          target = null
          const count = Math.min(buffer.byteLength, incoming.byteLength)
          buffer.set(incoming.subarray(0, count), 0)
          if (count < incoming.byteLength) {
            chunks.push(incoming.subarray(count))
          }
          resume(count)
          return
        }
        chunks.push(incoming)
      }
      channel.onerror = () => {
        if (!closed) {
          closed = true
          reject(new TypeError('WebSocket connection failed'))
        }
      }
      channel.onclose = () => {
        chunks.length = 0
        if (!opened && !closed) {
          closed = true
          reject(new TypeError('WebSocket connection failed'))
          return
        }
        closed = true
        if (waiter !== null) {
          const resume = waiter
          waiter = null
          target = null
          resume(null)
        }
      }
    })
  }

  /** Generate next unused frame identifier */
  #nextIdent(): number {
    let ident = (this.#sequence = (this.#sequence + 1) >>> 0)
    while (ident === 0 || this.#pending.has(ident)) {
      ident = this.#sequence = (this.#sequence + 1) >>> 0
    }
    return ident
  }

  /**
   * Shut down client and reject tickets.
   * @description Closes transport socket and rejects all pending requests.
   * @param reason - Error passed to pending request rejections
   */
  #shutdown(reason: Error): void {
    if (this.#closed) {
      return
    }
    this.#closed = true
    this.#cancelDelay?.()
    this.#writer.reset()
    for (const pending of this.#pending.values()) {
      clearTimeout(pending.timer)
      pending.reject(reason)
    }
    this.#pending.clear()
    try {
      this.#socket.close()
    } catch {
      void 0
    }
  }

  /**
   * Enqueue outbound request frame write.
   * @description Writes frame to socket and handles write failures.
   * @param ident - Request sequence identifier
   * @param frame - Encoded binary frame bytes
   * @param reject - Pending ticket rejection callback
   * @param timer - Active request timeout timer handle
   */
  #write(
    ident: number,
    frame: Uint8Array,
    reject: (reason: Error) => void,
    timer: ReturnType<typeof setTimeout>
  ): void {
    const socket = this.#socket
    this.#writer.enqueue(socket, frame).catch((caught) => {
      if (this.#reconnect > 0 && !this.#closed) {
        try {
          socket.close()
        } catch {
          void 0
        }
        return
      }
      clearTimeout(timer)
      if (!this.#pending.delete(ident)) {
        return
      }
      reject(caught instanceof Error ? caught : new TypeError(String(caught)))
    })
  }
}
