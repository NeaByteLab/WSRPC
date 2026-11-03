/**
 * Broadcast event envelope structure.
 * @description Carries event name and optional broadcast payload data.
 * @template TValue - Payload value type
 */
export interface RpcBroadcast<TValue = unknown> {
  /** Broadcast event name */
  e: string
  /** Optional broadcast payload data */
  d?: TValue
}

/**
 * RPC client connection options.
 * @description Configures target endpoint, timeouts, and reconnection attempts.
 */
export interface RpcClientOptions {
  /** Maximum frame payload byte length */
  frameLimit?: number
  /** Target server hostname or IP */
  hostname: string
  /** Target server TCP port */
  port: number
  /** Maximum reconnection attempts on disconnect */
  reconnect?: number
  /** Default request timeout in milliseconds */
  timeoutMs?: number
}

/**
 * RPC error response payload.
 * @description Contains error code and failure message string.
 */
export interface RpcFailure {
  /** Error status code string */
  code: string
  /** Error description message string */
  message: string
}

/**
 * Binary RPC transport frame.
 * @description Represents decoded frame header and raw payload bytes.
 */
export interface RpcFrame {
  /** Frame sequence identifier */
  ident: number
  /** Protocol operation code */
  opcode: RpcOpcode
  /** Raw frame payload bytes */
  payload: Uint8Array
}

/**
 * RPC method request envelope.
 * @description Carries procedure name and optional call parameters.
 * @template TValue - Request parameters type
 */
export interface RpcRequest<TValue = unknown> {
  /** Target procedure name */
  m: string
  /** Optional procedure call parameters */
  p?: TValue
}

/**
 * RPC server listener options.
 * @description Configures bind address, port, and frame size limit.
 */
export interface RpcServerOptions {
  /** Maximum frame payload byte length */
  frameLimit?: number
  /** Local bind hostname or IP */
  hostname?: string
  /** Local bind TCP port */
  port?: number
}

/**
 * Pending client request ticket.
 * @description Tracks active frame, settlement callbacks, and timeout timer.
 */
export interface RpcTicket {
  /** Encoded binary request frame */
  frame: Uint8Array
  /** True when awaiting pong response */
  isPing?: boolean
  /** Rejects pending request with error */
  reject: (reason: Error) => void
  /** Resolves pending request with result */
  resolve: (value: unknown) => void
  /** Active request timeout handle */
  timer: ReturnType<typeof setTimeout>
}

/**
 * Middleware guard transformation function.
 * @description Validates or transforms request parameters before handler execution.
 * @param params - Incoming request parameters
 * @returns Validated or transformed parameters
 * @template TInput - Input parameter type
 * @template TOutput - Transformed output parameter type
 */
export type RpcGuard<TInput = never, TOutput = unknown> = (
  params: TInput
) => Promise<TOutput> | TOutput

/**
 * RPC procedure handler function.
 * @description Executes registered procedure logic and returns response value.
 * @param params - Procedure call parameters
 * @returns Procedure execution result
 * @template TParams - Procedure parameter type
 * @template TResult - Procedure return value type
 */
export type RpcHandler<TParams = unknown, TResult = unknown> = (
  params: TParams
) => Promise<TResult> | TResult

/**
 * Broadcast event listener callback.
 * @description Handles incoming broadcast event payload on the client.
 * @param payload - Decoded event payload data
 * @returns Optional promise when listener is async
 * @template TData - Broadcast payload type
 */
export type RpcListener<TData = unknown> = (payload: TData) => Promise<void> | void

/** Binary frame operation code */
export type RpcOpcode = 0x01 | 0x02 | 0x03 | 0x04 | 0x05 | 0x06

/**
 * Overloaded route registration function.
 * @description Registers procedure handler with optional middleware guard chain.
 */
export type RpcRoute = {
  /**
   * Register procedure handler directly.
   * @description Binds method name to handler without middleware guards.
   * @param method - Procedure name to register
   * @param handler - Procedure execution callback
   * @template TParams - Procedure parameter type
   * @template TResult - Procedure return value type
   */
  <TParams, TResult>(method: string, handler: RpcHandler<TParams, TResult>): void
  /**
   * Register guarded procedure handler.
   * @description Binds method name to middleware guards and handler.
   * @param method - Procedure name to register
   * @param guards - Middleware guards run before handler
   * @param handler - Procedure execution callback
   * @template TParams - Procedure parameter type
   * @template TResult - Procedure return value type
   */
  <TParams, TResult>(
    method: string,
    guards: readonly RpcGuard[],
    handler: RpcHandler<TParams, TResult>
  ): void
}

/**
 * Server route setup callback.
 * @description Receives route registrar during server construction.
 * @param route - Route registration function
 */
export type RpcSetup = (route: RpcRoute) => void
