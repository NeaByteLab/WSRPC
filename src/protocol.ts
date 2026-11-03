import type * as Types from '@app/types.ts'

/**
 * Binary RPC wire protocol codec.
 * @description Encodes, decodes, and validates binary RPC transport frames.
 */
export default class Protocol {
  /** Default maximum payload byte length */
  static readonly limit = 16 * 1024 * 1024
  /** Binary protocol operation code map */
  static readonly opcode = {
    /** Server broadcast event opcode */
    broadcast: 0x06,
    /** Error response frame opcode */
    failure: 0x03,
    /** Keepalive ping request opcode */
    ping: 0x04,
    /** Keepalive pong response opcode */
    pong: 0x05,
    /** Procedure call request opcode */
    request: 0x01,
    /** Successful response frame opcode */
    success: 0x02
  } as const satisfies Record<string, Types.RpcOpcode>
  /** Shared text byte decoder */
  static readonly #decoder = new TextDecoder()
  /** Shared text string encoder */
  static readonly #encoder = new TextEncoder()
  /** Binary frame header byte length */
  static readonly #header = 9
  /** Maximum unsigned integer frame limit */
  static readonly #maximum = 0xffffffff
  /** Reusable encoding scratch buffer */
  static readonly #scratch = new Uint8Array(9 + 4096 * 3)

  /**
   * Configure TCP socket transport options.
   * @description Enables TCP nodelay and keepalive options when supported.
   * @param socket - Active Deno network connection
   */
  static configure(socket: Deno.Conn): void {
    const transport = socket as Partial<Deno.TcpConn>
    if (typeof transport.setNoDelay === 'function') {
      try {
        transport.setNoDelay(true)
      } catch {
        void 0
      }
    }
    if (typeof transport.setKeepAlive === 'function') {
      try {
        transport.setKeepAlive(true)
      } catch {
        void 0
      }
    }
  }

  /**
   * Decode JSON payload from bytes.
   * @description Parses encoded byte slice into typed JSON value.
   * @param bytes - Raw JSON payload bytes
   * @returns Parsed JSON payload value
   * @template TValue - Expected decoded value type
   */
  static decode<TValue = unknown>(bytes: Uint8Array): TValue {
    return JSON.parse(this.#decoder.decode(bytes)) as TValue
  }

  /**
   * Encode RPC frame into bytes.
   * @description Serializes header and optional JSON payload into frame.
   * @param opcode - Frame operation code
   * @param ident - Frame sequence identifier
   * @param payload - Optional serializable payload value
   * @param frameLimit - Maximum allowed payload byte length
   * @returns Encoded binary RPC frame
   * @throws {RangeError} When payload length exceeds frame limit
   */
  static encode(
    opcode: Types.RpcOpcode,
    ident: number,
    payload?: unknown,
    frameLimit: number = this.limit
  ): Uint8Array {
    const source = payload === undefined ? undefined : JSON.stringify(payload)
    if (source === undefined) {
      const frame = new Uint8Array(this.#header)
      frame[0] = opcode
      frame[1] = ident >>> 24
      frame[2] = ident >>> 16
      frame[3] = ident >>> 8
      frame[4] = ident
      return frame
    }
    if (source.length <= 4096) {
      const { written } = this.#encoder.encodeInto(source, this.#scratch.subarray(this.#header))
      if (written > frameLimit) {
        throw new RangeError(`Frame payload length ${written} exceeds maximum ${frameLimit}`)
      }
      this.#scratch[0] = opcode
      this.#scratch[1] = ident >>> 24
      this.#scratch[2] = ident >>> 16
      this.#scratch[3] = ident >>> 8
      this.#scratch[4] = ident
      this.#scratch[5] = written >>> 24
      this.#scratch[6] = written >>> 16
      this.#scratch[7] = written >>> 8
      this.#scratch[8] = written
      return this.#scratch.slice(0, this.#header + written)
    }
    const encoded = this.#encoder.encode(source)
    const length = encoded.byteLength
    if (length > frameLimit || length > this.#maximum) {
      throw new RangeError(`Frame payload length ${length} exceeds maximum ${frameLimit}`)
    }
    const frame = new Uint8Array(this.#header + length)
    frame[0] = opcode
    frame[1] = ident >>> 24
    frame[2] = ident >>> 16
    frame[3] = ident >>> 8
    frame[4] = ident
    frame[5] = length >>> 24
    frame[6] = length >>> 16
    frame[7] = length >>> 8
    frame[8] = length
    frame.set(encoded, this.#header)
    return frame
  }

  /**
   * Validate frame size limit value.
   * @description Ensures frame limit is a positive safe integer.
   * @param frameLimit - Maximum payload byte length to validate
   * @throws {RangeError} When frame limit is out of bounds
   */
  static validateLimit(frameLimit: number): void {
    if (!Number.isSafeInteger(frameLimit) || frameLimit <= 0 || frameLimit > this.#maximum) {
      throw new RangeError(`Frame limit must be an integer between 1 and ${this.#maximum}`)
    }
  }
}
