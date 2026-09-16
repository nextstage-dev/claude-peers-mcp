// Unique ID for each Claude Code instance (generated on registration)
export type PeerId = string;

export interface Peer {
  id: PeerId;
  nickname: string;
  context_window: number | null;
  context_used: number | null;
  context_note: string;
  tier: "production" | "staging" | "infrastructure";
  payload_version: number;
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  machine: string;
  summary: string;
  registered_at: string; // ISO timestamp
  last_seen: string; // ISO timestamp
  parent_id: string | null;
  runtime: string | null;
  rings: number[] | null;
  blocked_on: string | null;
  blocked_since: string | null;
}

export interface Message {
  id: number;
  from_id: PeerId;
  to_id: PeerId;
  text: string;
  sent_at: string; // ISO timestamp
  delivered: boolean;
  kind: MessageKind;
  reply_to_id: number | null;
}

export type MessageKind = "request" | "reply" | "notification";

// --- Broker API types ---

export interface RegisterRequest {
  requested_id: string;
  instance_id?: string;
  lease_id?: string;
  nickname?: string;
  context_window?: number | null;
  context_used?: number | null;
  context_note?: string;
  tier?: "production" | "staging" | "infrastructure";
  payload_version?: number;
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  machine: string;
  summary: string;
  parent_id?: string | null;
  runtime?: string | null;
  rings?: number[] | string | null;
}

export interface RegisterResponse {
  id: PeerId;
  role?: "owner" | "standby";
  lease_id?: string | null;
  lease_expires_at?: string;
}

export interface LeaseCredentials {
  instance_id?: string;
  lease_id?: string;
}

export interface HeartbeatRequest extends LeaseCredentials {
  id: PeerId;
  context_window?: number | null;
  context_used?: number | null;
}

export interface SetSummaryRequest extends LeaseCredentials {
  id: PeerId;
  summary: string;
}

export interface SetNicknameRequest extends LeaseCredentials {
  id: PeerId;
  nickname: string;
}

export interface SetContextRequest extends LeaseCredentials {
  id: PeerId;
  context_window?: number | null;
  context_used?: number | null;
  context_note?: string;
}

export interface SetStateRequest extends LeaseCredentials {
  id: PeerId;
  blocked_on?: string | null;
  blocked_since?: string | null;
}

export interface ListPeersRequest {
  scope: "machine" | "directory" | "repo" | "fleet";
  // The requesting peer's context (used for filtering)
  cwd: string;
  git_root: string | null;
  machine?: string;
  exclude_id?: PeerId;
  /**
   * When true, hide default zombie noise:
   * - no TTY (headless) AND summary matches idle/awaiting boilerplate
   * Orchestrators should default this on for tasking.
   */
  active_only?: boolean;
}

export interface SendMessageRequest extends LeaseCredentials {
  from_id: PeerId;
  to_id: PeerId;
  text: string;
  kind?: MessageKind;
  reply_to_id?: number;
  client_message_id?: string;
}

export interface SendMessageResponse {
  ok: boolean;
  message_id?: number;
  duplicate?: boolean;
  error?: string;
}

export interface PollMessagesRequest {
  id: PeerId;
}

export interface PollMessagesResponse {
  messages: Message[];
}

export interface ClaimMessagesRequest extends LeaseCredentials {
  id: PeerId;
}

export interface ClaimMessagesResponse {
  messages: Message[];
}

export interface AckMessagesRequest extends LeaseCredentials {
  id: PeerId;
  message_ids: number[];
}

export interface AckMessagesResponse {
  ok: boolean;
  acked: number;
}

export interface UnregisterRequest extends LeaseCredentials {
  id: PeerId;
}
