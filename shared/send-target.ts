/**
 * Target-argument handling for send_message, shared by every MCP entrypoint.
 *
 * send_message only ever took to_id, while its own description said "by peer ID"
 * and the delivery notification said `reply with send_message to "<id>"` —
 * neither naming the parameter. A caller reading those reasonably sent
 * {peer_id: "..."}; the handler destructured to_id as undefined, JSON.stringify
 * dropped the key, and the broker answered "Peer undefined not found", which
 * reads like the peer is gone rather than like the argument is misnamed.
 * Observed 2026-09-17 from ChatGPT through the MetaMCP gateway.
 *
 * to_id stays the official name: it is the mirror of the from_id every
 * notification carries and the broker's own column, so renaming only the
 * sending side would leave "you receive from_id but you send peer_id". peer_id
 * is therefore the alias, declared in the schema rather than merely tolerated,
 * so a client that validates against inputSchema still lets it through.
 */

export function normalizeSendTarget(args: unknown): string | null {
  const a = (args ?? {}) as { to_id?: unknown; peer_id?: unknown };
  const source = a.to_id ?? a.peer_id;
  if (typeof source !== "string") return null;
  const trimmed = source.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export const SEND_TARGET_HELP =
  'Pass the target peer ID as to_id (peer_id is accepted as an alias), e.g. {"to_id": "box-repo-042", "message": "..."}. ' +
  "It must be a registered peer ID as list_peers reports it — the same string that arrives as from_id on an inbound " +
  "message. A Claude web session_... URL id is not a peer ID.";

/** The two target properties every send_message inputSchema declares. */
export const SEND_TARGET_SCHEMA_PROPERTIES = {
  to_id: {
    type: "string" as const,
    description:
      "The peer ID of the target instance, exactly as list_peers reports it — the same string that arrives as from_id on an inbound message.",
  },
  peer_id: {
    type: "string" as const,
    description:
      'Alias for to_id. Accepted because the description says "by peer ID", so callers guessed this name; equivalent to to_id.',
  },
};
