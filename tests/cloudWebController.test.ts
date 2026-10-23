import { test } from 'node:test';
import assert from 'node:assert/strict';

import { buildCloudWebControllerConfig } from '../src/utils/cloudWebController.ts';

test('CloudWeb config defaults to the fake backend and controller name', () => {
  assert.deepEqual(
    buildCloudWebControllerConfig({
      name: 'CloudWeb',
      type: 'CloudWeb',
      display_short_side: 720,
    }),
    {
      type: 'CloudWeb',
      backend: 'fake',
      session_id: 'CloudWeb',
      display_short_side: 720,
    },
  );
});

test('CloudWeb config keeps explicit backend and session id', () => {
  assert.deepEqual(
    buildCloudWebControllerConfig({
      name: 'CloudWeb',
      type: 'CloudWeb',
      cloudweb: { backend: 'sidecar', session_id: 'session-a' },
    }),
    {
      type: 'CloudWeb',
      backend: 'sidecar',
      session_id: 'session-a',
      display_short_side: undefined,
    },
  );
});
