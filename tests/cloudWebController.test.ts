import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  buildCloudWebControllerConfig,
  createCloudWebSessionId,
  isCloudWebBackendSupported,
  resolveCloudWebSessionId,
} from '../src/utils/cloudWebController.ts';

test('CloudWeb config uses the fake backend, explicit session and profile', () => {
  assert.deepEqual(
    buildCloudWebControllerConfig(
      {
        name: 'CloudWeb',
        type: 'CloudWeb',
        cloudweb: { backend: 'fake', profile: 'default' },
        display_short_side: 720,
      },
      'session-1',
    ),
    {
      type: 'CloudWeb',
      backend: 'fake',
      session_id: 'session-1',
      profile: 'default',
      display_short_side: 720,
    },
  );
});

test('CloudWeb defaults to fake only when no backend is configured', () => {
  assert.deepEqual(
    buildCloudWebControllerConfig({ name: 'CloudWeb', type: 'CloudWeb' }, 'session-2'),
    {
      type: 'CloudWeb',
      backend: 'fake',
      session_id: 'session-2',
      profile: 'default',
      display_short_side: undefined,
    },
  );
});

test('CloudWeb rejects the unimplemented sidecar backend', () => {
  assert.equal(isCloudWebBackendSupported('sidecar'), false);
  assert.equal(
    buildCloudWebControllerConfig(
      {
        name: 'CloudWeb',
        type: 'CloudWeb',
        cloudweb: { backend: 'sidecar' },
      },
      'session-3',
    ),
    null,
  );
});

test('CloudWeb requires a non-empty session id', () => {
  assert.equal(buildCloudWebControllerConfig({ name: 'CloudWeb', type: 'CloudWeb' }, '  '), null);
});

test('CloudWeb session ids are non-empty and distinct', () => {
  const first = createCloudWebSessionId();
  const second = createCloudWebSessionId();
  assert.ok(first.length > 0);
  assert.notEqual(first, second);
});

test('CloudWeb reuses a saved session only for the same profile', () => {
  assert.equal(resolveCloudWebSessionId('session-a', 'default', 'default'), 'session-a');
  assert.notEqual(resolveCloudWebSessionId('session-a', 'default', 'alt'), 'session-a');
  assert.notEqual(resolveCloudWebSessionId('session-a', undefined, 'default'), 'session-a');
});
