import type * as Types from '@app/types.ts'
import Client from '@app/client.ts'
import Server from '@app/server.ts'

/**
 * WSRPC client and server module.
 * @description Exports RPC client factory and server class constructor.
 */
export default {
  /**
   * Connect to remote RPC server.
   * @description Creates and connects a new RPC client instance.
   * @param options - Client connection configuration options
   * @returns Promise resolving with connected Client instance
   */
  Client: (options: Types.RpcClientOptions): Promise<Client> => Client.connect(options),
  /** RPC server class constructor */
  Server
} as const

/** Export public RPC type definitions */
export type * from '@app/types.ts'
