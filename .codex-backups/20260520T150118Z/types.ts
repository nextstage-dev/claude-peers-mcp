// Unique ID for each Claude Code instance (generated on registration)
export type PeerId = string;

export interface Peer {
  id: PeerId;
  nickname: string;
  context_window: number | null;
  context_used: number | null;
  context_note: string;
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  machine: string;
  summary: string;
  registered_at: string; // ISO timestamp
  last_seen: string; // ISO timestamp
}

export interface Message {
  id: number;
  from_id: PeerId;
  to_id: PeerId;
  text: string;
  sent_at: string; // ISO timestamp
  delivered: boolean;
}

// --- Broker API types ---

export interface RegisterRequest {
  requested_id?: string;
  nickname?: string;
  context_window?: number | null;
  context_used?: number | null;
  context_note?: string;
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  machine: string;
  summary: string;
}

export interface RegisterResponse {
  id: PeerId;
}

export interface HeartbeatRequest {
  id: PeerId;
}

export interface SetSummaryRequest {
  id: PeerId;
  summary: string;
}

export interface SetNicknameRequest {
  id: PeerId;
  nickname: string;
}

export interface SetContextRequest {
  id: PeerId;
  context_window?: number | null;
  context_used?: number | null;
  context_note?: string;
}

export interface ListPeersRequest {
  scope: "machine" | "directory" | "repo" | "fleet";
  // The requesting peer's context (used for filtering)
  cwd: string;
  git_root: string | null;
  machine?: string;
  exclude_id?: PeerId;
}

export interface SendMessageRequest {
  from_id: PeerId;
  to_id: PeerId;
  text: string;
}

export interface PollMessagesRequest {
  id: PeerId;
}

export interface PollMessagesResponse {
  messages: Message[];
}
