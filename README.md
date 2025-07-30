// Summary:
// This file implements a minimal Redis-like server in C++ supporting basic commands (SET, GET, INCR, ECHO, MULTI/EXEC, TYPE, KEYS, CONFIG, PING, QUIT), simple replication, and partial stream (XADD) support.
// - Uses TCP sockets for networking and threads for concurrency.
// - Maintains an in-memory key-value store with optional expiry.
// - Supports Redis streams with explicit and partial auto-generated IDs for XADD.
// - Handles replication by propagating commands to a replica over a dedicated connection, suppressing responses to the replica except for handshake commands.
// - RESP protocol parsing and serialization is implemented for communication.
// - RDB file loading is stubbed for persistence simulation.
// - Command handling is done in a single function with per-connection state for transactions.