import type { ControllerItem } from '@/types/interface';
import type { CloudWebControllerConfig } from '@/types/maa';

export function buildCloudWebControllerConfig(
  controller: ControllerItem | undefined,
): CloudWebControllerConfig {
  return {
    type: 'CloudWeb',
    backend: controller?.cloudweb?.backend || 'fake',
    session_id: controller?.cloudweb?.session_id || controller?.name || 'cloudweb',
    display_short_side: controller?.display_short_side,
  };
}
