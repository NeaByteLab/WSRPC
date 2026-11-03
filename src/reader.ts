import type * as Types from '@app/types.ts'
import Protocol from '@app/protocol.ts'

/**
 * Buffered TCP and WebSocket reader.
 * @description Reads RPC frames from raw TCP or WebSocket streams.
 */
export default class Reader {
  /** Default internal buffer byte capacity */
  static readonly #capacity = 64 * 1024
  /** Fatal text decoder for validation */
  static readonly #decoder = new TextDecoder('utf-8', { fatal: true })
  /** Shared text string encoder */
  static readonly #encoder = new TextEncoder()
  /** Minimum free buffer space threshold */
  static readonly #minimum = 4096
  /** Maximum allowed frame payload length */
  readonly #frameLimit: number
  /** Underlying network socket connection */
  readonly #socket: Deno.Conn
  /** Bound raw socket write function */
  readonly #writer: ((chunk: Uint8Array) => Promise<number>) | null
  /** Internal byte accumulation buffer */
  #buffer: Uint8Array
  /** DataView over accumulation buffer */
  #header: DataView
  /** Current buffer read offset */
  #readIndex = 0
  /** WebSocket upgrade state indicator */
  #upgraded: number | Promise<boolean> | null = null
  /** Serialized WebSocket packet write chain */
  #writeChain: Promise<void> = Promise.resolve()
  /** Current buffer write offset */
  #writeIndex = 0

  /**
   * Create buffered stream frame reader.
   * @description Initializes read buffer and wraps socket write for WebSocket.
   * @param socket - Underlying network connection
   * @param frameLimit - Maximum allowed frame payload byte length
   */
  constructor(socket: Deno.Conn, frameLimit: number = Protocol.limit) {
    Protocol.validateLimit(frameLimit)
    this.#socket = socket
    this.#frameLimit = frameLimit
    this.#buffer = new Uint8Array(Reader.#capacity)
    this.#header = new DataView(
      this.#buffer.buffer,
      this.#buffer.byteOffset,
      this.#buffer.byteLength
    )
    this.#writer = typeof this.#socket.write === 'function'
      ? this.#socket.write.bind(this.#socket)
      : null
    if (this.#writer !== null) {
      const writer = this.#writer
      Object.defineProperty(this.#socket, 'write', {
        configurable: true,
        writable: true,
        value: async (chunk: Uint8Array): Promise<number> => {
          if (this.#upgraded === null && chunk[0] === Protocol.opcode.broadcast) {
            await new Promise((resolve) => setTimeout(resolve, 5))
          }
          if (this.#upgraded instanceof Promise) {
            await this.#upgraded
          }
          if (this.#upgraded === -1) {
            return 0
          }
          if (this.#upgraded !== 1) {
            return await writer(chunk)
          }
          const length = chunk.byteLength
          const prefix = length < 126 ? 2 : length <= 0xffff ? 4 : 10
          const packet = new Uint8Array(prefix + length)
          packet[0] = 0x82
          if (length < 126) {
            packet[1] = length
          } else if (length <= 0xffff) {
            packet[1] = 126
            packet[2] = length >>> 8
            packet[3] = length
          } else {
            packet[1] = 127
            packet[6] = length >>> 24
            packet[7] = length >>> 16
            packet[8] = length >>> 8
            packet[9] = length
          }
          packet.set(chunk, prefix)
          return (await this.#writePacket(packet)) ? length : 0
        }
      })
    }
  }

  /** Read next RPC frame */
  async readFrame(): Promise<Types.RpcFrame | null> {
    if (this.#upgraded === null) {
      if (!(await this.#bufferBytes(4))) {
        return null
      }
      if (
        this.#buffer[this.#readIndex] === 0x47 &&
        this.#buffer[this.#readIndex + 1] === 0x45 &&
        this.#buffer[this.#readIndex + 2] === 0x54 &&
        this.#buffer[this.#readIndex + 3] === 0x20
      ) {
        let resolveUpgrade!: (value: boolean) => void
        this.#upgraded = new Promise<boolean>((resolve) => {
          resolveUpgrade = resolve
        })
        try {
          while (true) {
            const unread = this.#writeIndex - this.#readIndex
            let boundary = -1
            for (let index = this.#readIndex; index + 3 < this.#writeIndex; index += 1) {
              if (
                this.#buffer[index] === 0x0d &&
                this.#buffer[index + 1] === 0x0a &&
                this.#buffer[index + 2] === 0x0d &&
                this.#buffer[index + 3] === 0x0a
              ) {
                boundary = index
                break
              }
            }
            if (boundary !== -1) {
              const headerText = new TextDecoder().decode(
                this.#buffer.subarray(this.#readIndex, boundary + 4)
              )
              const matched = /^sec-websocket-key:\s*([A-Za-z0-9+/]{22}==)\s*\r?$/im.exec(
                headerText
              )
              if (
                !/^GET\s+\/\S*\s+HTTP\/1\.1\r\n/.test(headerText) ||
                !/^host:\s*\S+/im.test(headerText) ||
                !matched ||
                !matched[1] ||
                !/^upgrade:[^\r\n]*\bwebsocket\b/im.test(headerText) ||
                !/^connection:[^\r\n]*\bupgrade\b/im.test(headerText) ||
                !/^sec-websocket-version:\s*13\s*\r?$/im.test(headerText)
              ) {
                throw new RangeError('Invalid WebSocket upgrade request')
              }
              this.#readIndex = boundary + 4
              this.#rewind()
              if (
                !(await this.#writePacket(
                  Reader.#encoder.encode(
                    `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${
                      btoa(
                        String.fromCharCode(
                          ...new Uint8Array(
                            await crypto.subtle.digest(
                              'SHA-1',
                              Reader.#encoder.encode(
                                `${matched[1]}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`
                              )
                            )
                          )
                        )
                      )
                    }\r\n\r\n`
                  )
                ))
              ) {
                throw new RangeError('Failed to write WebSocket handshake response')
              }
              this.#upgraded = 1
              resolveUpgrade(true)
              break
            }
            if (unread >= 8192 || !(await this.#bufferBytes(unread + 1))) {
              throw new RangeError('Invalid WebSocket upgrade header')
            }
          }
        } catch (caught) {
          this.#upgraded = -1
          resolveUpgrade(false)
          throw caught
        }
      } else {
        this.#upgraded = 0
      }
    }
    if (this.#upgraded === -1) {
      return null
    }
    if (this.#upgraded === 1) {
      let controlCount = 0
      let merged: Uint8Array | null = null
      let mergedLength = 0
      let targetLength = -1
      while (true) {
        if (!(await this.#bufferBytes(2))) {
          if (merged !== null) {
            throw new RangeError('WebSocket fragmented frame ended before final fragment')
          }
          return null
        }
        const firstByte = this.#header.getUint8(this.#readIndex)
        const secondByte = this.#header.getUint8(this.#readIndex + 1)
        this.#readIndex += 2
        if ((firstByte & 0x70) !== 0) {
          throw new RangeError('Invalid WebSocket reserved bits')
        }
        const frameType = firstByte & 0x0f
        if ((secondByte & 0x80) === 0) {
          throw new RangeError('Unmasked WebSocket client frame')
        }
        let totalLength = secondByte & 0x7f
        if (frameType === 0x08) {
          if ((firstByte & 0x80) === 0 || totalLength === 1 || totalLength > 125) {
            throw new RangeError('Invalid WebSocket control frame')
          }
          if (!(await this.#bufferBytes(4 + totalLength))) {
            throw new RangeError('Frame payload ended before the declared length was read')
          }
          const maskOffset = this.#readIndex
          const bodyOffset = this.#readIndex + 4
          for (let index = 0; index < totalLength; index += 1) {
            this.#buffer[bodyOffset + index] = (this.#buffer[bodyOffset + index] ?? 0) ^
              (this.#buffer[maskOffset + (index & 3)] ?? 0)
          }
          const closeFrame = new Uint8Array(totalLength >= 2 ? 4 : 2)
          closeFrame[0] = 0x88
          if (totalLength >= 2) {
            const status = this.#header.getUint16(bodyOffset, false)
            if (
              status < 1000 ||
              status === 1004 ||
              status === 1005 ||
              status === 1006 ||
              (status >= 1014 && status < 3000) ||
              status >= 5000
            ) {
              throw new RangeError(`Invalid WebSocket close status ${status}`)
            }
            if (totalLength > 2) {
              try {
                Reader.#decoder.decode(
                  this.#buffer.subarray(bodyOffset + 2, bodyOffset + totalLength)
                )
              } catch {
                throw new RangeError('Invalid UTF-8 in WebSocket close reason')
              }
            }
            closeFrame[1] = 2
            closeFrame[2] = this.#buffer[bodyOffset] ?? 0
            closeFrame[3] = this.#buffer[bodyOffset + 1] ?? 0
          }
          this.#readIndex = bodyOffset + totalLength
          this.#rewind()
          this.#upgraded = -1
          await this.#writePacket(closeFrame).catch(() => false)
          return null
        }
        if (frameType === 0x09 || frameType === 0x0a) {
          controlCount += 1
          if ((firstByte & 0x80) === 0 || totalLength > 125 || controlCount > 16) {
            throw new RangeError('Invalid WebSocket control frame')
          }
          if (!(await this.#bufferBytes(4 + totalLength))) {
            throw new RangeError('Frame payload ended before the declared length was read')
          }
          if (frameType === 0x09 && this.#writer !== null) {
            const maskOffset = this.#readIndex
            const bodyOffset = this.#readIndex + 4
            const pongFrame = new Uint8Array(2 + totalLength)
            pongFrame[0] = 0x8a
            pongFrame[1] = totalLength
            for (let index = 0; index < totalLength; index += 1) {
              pongFrame[2 + index] = (this.#buffer[bodyOffset + index] ?? 0) ^
                (this.#buffer[maskOffset + (index & 3)] ?? 0)
            }
            this.#readIndex = bodyOffset + totalLength
            this.#rewind()
            await this.#writePacket(pongFrame).catch(() => false)
          } else {
            this.#readIndex += 4 + totalLength
            this.#rewind()
          }
          continue
        }
        if (merged === null ? frameType !== 0x02 : frameType !== 0x00) {
          throw new RangeError(`Invalid WebSocket frame opcode ${frameType}`)
        }
        controlCount = 0
        if (totalLength === 126) {
          if (!(await this.#bufferBytes(2))) {
            throw new RangeError('WebSocket frame ended inside extended length')
          }
          totalLength = this.#header.getUint16(this.#readIndex, false)
          if (totalLength < 126) {
            throw new RangeError('Non-minimal WebSocket extended payload length')
          }
          this.#readIndex += 2
        } else if (totalLength === 127) {
          if (!(await this.#bufferBytes(8))) {
            throw new RangeError('WebSocket frame ended inside extended length')
          }
          if (this.#header.getUint32(this.#readIndex, false) !== 0) {
            throw new RangeError('WebSocket frame exceeds 32-bit maximum')
          }
          totalLength = this.#header.getUint32(this.#readIndex + 4, false)
          if (totalLength <= 0xffff) {
            throw new RangeError('Non-minimal WebSocket extended payload length')
          }
          this.#readIndex += 8
        }
        if ((firstByte & 0x80) === 0 && totalLength === 0) {
          throw new RangeError('Empty WebSocket continuation fragment')
        }
        const combinedLength = mergedLength + totalLength
        if (combinedLength - 9 > this.#frameLimit) {
          throw new RangeError(
            `Frame payload length ${combinedLength - 9} exceeds maximum ${this.#frameLimit}`
          )
        }
        if (!(await this.#bufferBytes(4 + totalLength))) {
          throw new RangeError('Frame payload ended before the declared length was read')
        }
        const maskOffset = this.#readIndex
        const bodyOffset = this.#readIndex + 4
        for (let index = 0; index < totalLength; index += 1) {
          this.#buffer[bodyOffset + index] = (this.#buffer[bodyOffset + index] ?? 0) ^
            (this.#buffer[maskOffset + (index & 3)] ?? 0)
        }
        if ((firstByte & 0x80) === 0 || merged !== null) {
          if (merged === null) {
            merged = new Uint8Array(Math.max(combinedLength, 64))
          } else if (merged.byteLength < combinedLength) {
            let nextCapacity: number = merged.byteLength * 2
            while (nextCapacity < combinedLength) {
              nextCapacity *= 2
            }
            const grown: Uint8Array = new Uint8Array(
              targetLength >= 0 ? Math.min(nextCapacity, 9 + targetLength) : nextCapacity
            )
            grown.set(merged.subarray(0, mergedLength), 0)
            merged = grown
          }
          merged.set(this.#buffer.subarray(bodyOffset, bodyOffset + totalLength), mergedLength)
          if (targetLength < 0 && combinedLength >= 9) {
            const headerView: DataView = new DataView(merged.buffer, merged.byteOffset, 9)
            const earlyOpcode = headerView.getUint8(0)
            targetLength = headerView.getUint32(5, false)
            if (earlyOpcode < Protocol.opcode.request || earlyOpcode > Protocol.opcode.broadcast) {
              throw new RangeError(`Invalid frame opcode ${earlyOpcode}`)
            }
            if (targetLength > this.#frameLimit || combinedLength - 9 > targetLength) {
              throw new RangeError(
                `Frame payload length ${targetLength} exceeds maximum ${this.#frameLimit}`
              )
            }
          } else if (targetLength >= 0 && combinedLength - 9 > targetLength) {
            throw new RangeError(
              `Frame payload length ${combinedLength - 9} exceeds maximum ${this.#frameLimit}`
            )
          }
          mergedLength = combinedLength
          this.#readIndex = bodyOffset + totalLength
          this.#rewind()
          if ((firstByte & 0x80) === 0) {
            continue
          }
        }
        const frameOffset = merged !== null ? 0 : bodyOffset
        const viewHeader = merged !== null
          ? new DataView(merged.buffer, merged.byteOffset, mergedLength)
          : this.#header
        if (combinedLength < 9) {
          throw new RangeError('WebSocket frame smaller than RPC header')
        }
        const opcode = viewHeader.getUint8(frameOffset) as Types.RpcOpcode
        if (opcode < Protocol.opcode.request || opcode > Protocol.opcode.broadcast) {
          throw new RangeError(`Invalid frame opcode ${opcode}`)
        }
        const ident = viewHeader.getUint32(frameOffset + 1, false)
        const length = viewHeader.getUint32(frameOffset + 5, false)
        if (length > this.#frameLimit || length !== combinedLength - 9) {
          throw new RangeError(`Frame payload length ${length} exceeds maximum ${this.#frameLimit}`)
        }
        const payload = length === 0
          ? new Uint8Array(0)
          : (merged !== null ? merged : this.#buffer).slice(
            frameOffset + 9,
            frameOffset + 9 + length
          )
        if (merged === null) {
          this.#readIndex = bodyOffset + totalLength
          this.#rewind()
        }
        return { opcode, ident, payload }
      }
    }
    if (!(await this.#bufferBytes(9))) {
      return null
    }
    const opcode = this.#header.getUint8(this.#readIndex) as Types.RpcOpcode
    if (opcode < Protocol.opcode.request || opcode > Protocol.opcode.broadcast) {
      throw new RangeError(`Invalid frame opcode ${opcode}`)
    }
    const ident = this.#header.getUint32(this.#readIndex + 1, false)
    const length = this.#header.getUint32(this.#readIndex + 5, false)
    if (length > this.#frameLimit) {
      throw new RangeError(`Frame payload length ${length} exceeds maximum ${this.#frameLimit}`)
    }
    this.#readIndex += 9
    if (length === 0) {
      this.#rewind()
      return { opcode, ident, payload: new Uint8Array(0) }
    }
    if (!(await this.#bufferBytes(length))) {
      throw new RangeError('Frame payload ended before the declared length was read')
    }
    const payload = this.#buffer.slice(this.#readIndex, this.#readIndex + length)
    this.#readIndex += length
    this.#rewind()
    return { opcode, ident, payload }
  }

  /**
   * Ensure buffer has needed bytes.
   * @description Compacts or grows buffer and reads from socket stream.
   * @param needed - Minimum unread byte count required
   * @returns True when bytes are available, false on EOF
   * @throws {RangeError} When stream ends early or returns zero bytes
   */
  async #bufferBytes(needed: number): Promise<boolean> {
    while (this.#writeIndex - this.#readIndex < needed) {
      const unread = this.#writeIndex - this.#readIndex
      const missing = needed - unread
      if (this.#buffer.byteLength - this.#writeIndex < missing) {
        if (
          this.#readIndex > 0 &&
          this.#buffer.byteLength > Reader.#capacity &&
          needed <= Reader.#capacity - Reader.#minimum
        ) {
          const shrunk = new Uint8Array(Reader.#capacity)
          if (unread > 0) {
            shrunk.set(this.#buffer.subarray(this.#readIndex, this.#writeIndex), 0)
          }
          this.#buffer = shrunk
          this.#header = new DataView(
            this.#buffer.buffer,
            this.#buffer.byteOffset,
            this.#buffer.byteLength
          )
          this.#writeIndex = unread
          this.#readIndex = 0
        } else if (
          this.#readIndex > 0 &&
          (
            this.#buffer.byteLength - unread >= missing ||
            this.#buffer.byteLength - unread >= Reader.#minimum
          )
        ) {
          if (unread > 0) {
            this.#buffer.copyWithin(0, this.#readIndex, this.#writeIndex)
          }
          this.#writeIndex = unread
          this.#readIndex = 0
        } else if (
          this.#readIndex > 0 || this.#buffer.byteLength - this.#writeIndex < Reader.#minimum
        ) {
          const expanded = new Uint8Array(this.#buffer.byteLength * 2)
          expanded.set(this.#buffer.subarray(this.#readIndex, this.#writeIndex), 0)
          this.#buffer = expanded
          this.#header = new DataView(
            this.#buffer.buffer,
            this.#buffer.byteOffset,
            this.#buffer.byteLength
          )
          this.#writeIndex = unread
          this.#readIndex = 0
        }
      }
      const received = await this.#socket.read(this.#buffer.subarray(this.#writeIndex))
      if (received === null) {
        if (this.#writeIndex - this.#readIndex === 0) {
          return false
        }
        throw new RangeError('TCP frame ended before the declared bytes were read')
      }
      if (received === 0) {
        throw new RangeError('Zero-byte read from TCP stream')
      }
      this.#writeIndex += received
    }
    return true
  }

  /** Reset buffer offsets when empty */
  #rewind(): void {
    if (this.#readIndex === this.#writeIndex) {
      this.#readIndex = 0
      this.#writeIndex = 0
      if (this.#buffer.byteLength > Reader.#capacity) {
        this.#buffer = new Uint8Array(Reader.#capacity)
        this.#header = new DataView(
          this.#buffer.buffer,
          this.#buffer.byteOffset,
          this.#buffer.byteLength
        )
      }
    }
  }

  /**
   * Write raw packet to socket.
   * @description Serializes packet writes through internal promise chain.
   * @param packet - Raw packet bytes to send
   * @returns True when all packet bytes were written
   */
  #writePacket(packet: Uint8Array): Promise<boolean> {
    const writer = this.#writer
    if (writer === null) {
      return Promise.resolve(false)
    }
    const result = this.#writeChain.then(async () => {
      let total = 0
      while (total < packet.byteLength) {
        const writtenBytes = await writer(total === 0 ? packet : packet.subarray(total))
        if (writtenBytes <= 0) {
          return false
        }
        total += writtenBytes
      }
      return true
    })
    this.#writeChain = result.then(
      () => {},
      () => {}
    )
    return result
  }
}
