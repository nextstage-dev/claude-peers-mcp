import type { SendMessageRequest, SendMessageResponse } from "./types.ts";

export const messageContractVersion = 1;
export const messageToolProperties = {
  kind: { type: "string" as const, enum: ["request", "reply", "notification"], description: "request asks for work; reply answers an earlier request; notification needs no answer. Default request." },
  reply_to_id: { type: "integer" as const, minimum: 1, description: "Original message ID when replying. Sets kind=reply if omitted." },
  client_message_id: { type: "string" as const, minLength: 1, maxLength: 128, description: "Reuse the same ID when retrying this exact send; a changed message requires a new ID." },
};

// Keep transport metadata out of message text. Never silently discard it on an older broker.
export async function sendPeerMessage(
  call: (route: string, body: any) => Promise<any>,
  body: SendMessageRequest,
  args: Record<string, unknown> = {},
): Promise<SendMessageResponse> {
  const metadata = Object.fromEntries(["kind", "reply_to_id", "client_message_id"]
    .filter(key => args[key] !== undefined).map(key => [key, args[key]]));
  if (Object.keys(metadata).length) {
    const capabilities = await call("/capabilities", {});
    if (capabilities.message_contract !== messageContractVersion) throw new Error("Broker must support message contract v1 before sending metadata");
  }
  return call("/send-message", { ...body, ...metadata });
}
