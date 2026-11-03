/**
 * Sequential socket frame write queue.
 * @description Serializes outbound frame writes across a single connection.
 */
export default class Queue {
  /** Active sequential write promise chain */
  #chain: Promise<void> | null = null

  /**
   * Enqueue frame for sequential write.
   * @description Chains frame write after any pending socket writes.
   * @param socket - Target network connection
   * @param frame - Binary frame bytes to write
   * @returns Promise resolving when frame is fully written
   */
  enqueue(socket: Deno.Conn, frame: Uint8Array): Promise<void> {
    const pending = this.#chain === null
      ? Queue.#write(socket, frame)
      : this.#chain.then(() => Queue.#write(socket, frame))
    const finish = (): void => {
      if (this.#chain === tracked) {
        this.#chain = null
      }
    }
    const tracked = pending.then(finish, finish)
    this.#chain = tracked
    return pending
  }

  /** Reset active write promise chain */
  reset(): void {
    this.#chain = null
  }

  /**
   * Write full frame to socket.
   * @description Loops until all frame bytes are written to stream.
   * @param socket - Target network connection
   * @param frame - Binary frame bytes to write
   * @returns Promise resolving when write completes
   * @throws {RangeError} When socket write returns zero bytes
   */
  static async #write(socket: Deno.Conn, frame: Uint8Array): Promise<void> {
    let written = 0
    while (written < frame.byteLength) {
      const count = await socket.write(written === 0 ? frame : frame.subarray(written))
      if (count <= 0) {
        throw new RangeError('Failed to write frame bytes to TCP stream')
      }
      written += count
    }
  }
}
