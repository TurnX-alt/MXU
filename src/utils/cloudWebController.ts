import type { ControllerItem } from '@/types/interface';
import type { CloudWebControllerConfig } from '@/types/maa';

/** Backends accepted by the current Rust Host factory. */
const SUPPORTED_BACKENDS = new Set(['fake', 'sidecar']);

export function isCloudWebBackendSupported(backend: string | undefined): backend is 'sidecar' | 'fake' {
  return backend !== undefined && SUPPORTED_BACKENDS.has(backend);
}


export function createCloudWebSessionId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `cloudweb-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function resolveCloudWebSessionId(
  savedSessionId: string | undefined,
  savedProfile: string | undefined,
  profile: string,
): string {
  return savedSessionId && savedProfile === profile ? savedSessionId : createCloudWebSessionId();
}

export function buildCloudWebControllerConfig(
  controller: ControllerItem | undefined,
  sessionId: string,
): CloudWebControllerConfig | null {
  // An unconfigured CloudWeb controller uses the real sidecar backend; the fake
  // stub is only reachable when an interface explicitly selects it.
  const backend = controller?.cloudweb?.backend ?? 'sidecar';

  if (!isCloudWebBackendSupported(backend)) {
    return null;
  }
  const session = sessionId.trim();
  if (!session) {
    return null;
  }
  return {
    type: 'CloudWeb',
    backend,
    session_id: session,
    profile: controller?.cloudweb?.profile || 'default',
    display_short_side: controller?.display_short_side,
  };
}
