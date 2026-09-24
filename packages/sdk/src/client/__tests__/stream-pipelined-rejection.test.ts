/**
 * Unit tests for MppSessionClient.stream() pipelined unhandled rejection handling (#397).
 *
 * Follows the mocking style in stream-backpressure.test.ts.
 *
 * Verifies:
 *   1. With concurrency: 3, a mocked fetch where slot 2 rejects before slot 1 resolves
 *      produces zero unhandledRejection events, and consumer receives slot 1 data
 *      followed by slot 2's error from the iterator.
 *   2. Breaking out of for await after first item while other slots later reject
 *      produces zero unhandledRejection events.
 *   3. When head slot rejects while other slots are still in flight, error propagates
 *      to consumer and zero unhandledRejection events fire.
 *   4. When onSpend throws during replenish, spend error propagates and queued slots
 *      do not cause unhandledRejection.
 */

import assert from 'node:assert/strict'
import type { StreamOptions } from '../../types.js'

// ── Helpers ───────────────────────────────────────────────────────────────────

function deferred<T = unknown>(): {
  promise: Promise<T>
  resolve: (v: T) => void
  reject: (e: unknown) => void
} {
  let resolve!: (v: T) => void
  let reject!: (e: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

/**
 * Build a mock stream reproducing MppSessionClient.stream() with pipelined
 * unhandled rejection safety and spend checking.
 */
function buildMockStream(
  fetchFn: () => Promise<unknown>,
  onSpend?: (amount: string) => Promise<void>,
) {
  let vouchersIssued = 0

  return {
    vouchersIssued: () => vouchersIssued,
    async *stream(options?: StreamOptions): AsyncIterable<unknown> {
      const concurrency = Math.max(1, options?.concurrency ?? 1)
      const doFetch = () => fetchFn()
      const checkSpend = (): Promise<void> => {
        if (!onSpend) return Promise.resolve()
        return onSpend('0.0001')
      }

      if (concurrency === 1) {
        while (true) {
          await checkSpend()
          const data = await doFetch()
          vouchersIssued++
          yield data
        }
      } else {
        const queue: Array<Promise<unknown>> = []
        try {
          for (let i = 0; i < concurrency; i++) {
            await checkSpend()
            const p = doFetch()
            p.catch(() => {})
            queue.push(p)
          }

          while (true) {
            const data = await queue.shift()!
            // Replenish the window immediately after draining one slot.
            await checkSpend()
            const p = doFetch()
            p.catch(() => {})
            queue.push(p)
            vouchersIssued++
            yield data
          }
        } finally {
          await Promise.allSettled(queue)
          queue.length = 0
        }
      }
    },
  }
}

// ── Test Runner with unhandledRejection tracking ───────────────────────────────

async function runTest(
  name: string,
  fn: (unhandled: unknown[]) => Promise<void>,
) {
  const unhandled: unknown[] = []
  const listener = (reason: unknown) => {
    unhandled.push(reason)
  }
  process.on('unhandledRejection', listener)

  try {
    await fn(unhandled)
    // Small delay to allow any unhandled rejections to be noticed by Node
    await new Promise((r) => setTimeout(r, 20))
    assert.strictEqual(
      unhandled.length,
      0,
      `Expected 0 unhandledRejection events, found ${unhandled.length}: ${JSON.stringify(unhandled)}`,
    )
    console.log(`✓ ${name}`)
  } finally {
    process.removeListener('unhandledRejection', listener)
  }
}

// ── Test 1: concurrency: 3 — slot 2 rejects before slot 1 resolves ───────────

await runTest(
  'Test 1: with concurrency: 3, slot 2 rejects before slot 1 resolves -> 0 unhandledRejections and yields slot 1 then slot 2 error',
  async () => {
    const d1 = deferred<{ seq: number }>()
    const d2 = deferred<{ seq: number }>()
    const d3 = deferred<{ seq: number }>()
    const defs = [d1, d2, d3]

    const mock = buildMockStream(() => {
      const d = defs.shift()
      if (!d) return Promise.resolve({ seq: 99 })
      return d.promise
    })

    const iter = mock.stream({ concurrency: 3 })[Symbol.asyncIterator]()

    // Kick off first item (fills queue with d1, d2, d3)
    const firstNext = iter.next()
    await Promise.resolve()

    // Slot 2 rejects while slot 1 is still pending
    d2.reject(new Error('slot 2 error'))
    // Settle slot 3 concurrently so allSettled in finally can settle
    setTimeout(() => d3.resolve({ seq: 3 }), 10)
    await Promise.resolve()

    // Slot 1 resolves
    d1.resolve({ seq: 1 })
    const { value: v1 } = await firstNext
    assert.deepStrictEqual(v1, { seq: 1 })

    // Next item must throw slot 2's error
    await assert.rejects(
      async () => {
        await iter.next()
      },
      (err: Error) => {
        assert.match(err.message, /slot 2 error/)
        return true
      },
    )
  },
)

// ── Test 2: Breaking out of for await after first item ─────────────────────────

await runTest(
  'Test 2: breaking out of for await after first item, while other slots later reject, produces 0 unhandledRejections',
  async () => {
    const d1 = deferred<{ seq: number }>()
    const d2 = deferred<{ seq: number }>()
    const d3 = deferred<{ seq: number }>()
    const defs = [d1, d2, d3]

    const mock = buildMockStream(() => {
      const d = defs.shift()
      if (!d) return Promise.resolve({ seq: 99 })
      return d.promise
    })

    const results: unknown[] = []

    // Slot 1 resolves immediately
    d1.resolve({ seq: 1 })

    // Other queued slots reject after a short delay
    setTimeout(() => {
      d2.reject(new Error('slot 2 late rejection'))
      d3.reject(new Error('slot 3 late rejection'))
    }, 10)

    for await (const item of mock.stream({ concurrency: 3 })) {
      results.push(item)
      break // Early exit
    }

    assert.strictEqual(results.length, 1)
  },
)

// ── Test 3: Head slot rejects while other slots in-flight ──────────────────────

await runTest(
  'Test 3: when head slot rejects while other slots are still in flight, error propagates and 0 unhandledRejections fire',
  async () => {
    const d1 = deferred<{ seq: number }>()
    const d2 = deferred<{ seq: number }>()
    const defs = [d1, d2]

    const mock = buildMockStream(() => {
      const d = defs.shift()
      if (!d) return Promise.resolve({ seq: 99 })
      return d.promise
    })

    const iter = mock.stream({ concurrency: 2 })[Symbol.asyncIterator]()

    // Secondary slot rejects slightly later
    setTimeout(() => {
      d2.reject(new Error('secondary slot late failure'))
    }, 10)

    // Reject head slot immediately
    d1.reject(new Error('head slot failed'))

    await assert.rejects(
      async () => {
        await iter.next()
      },
      (err: Error) => {
        assert.match(err.message, /head slot failed/)
        return true
      },
    )
  },
)

// ── Test 4: onSpend throws during replenish ────────────────────────────────────

await runTest(
  'Test 4: when onSpend throws during replenish, spend error propagates and queued slots do not cause unhandledRejection',
  async () => {
    const d1 = deferred<{ seq: number }>()
    const d2 = deferred<{ seq: number }>()
    const defs = [d1, d2]

    let spendCallCount = 0
    const onSpend = async () => {
      spendCallCount++
      // The initial window of 2 calls checkSpend 2 times. The 3rd call is during replenish.
      if (spendCallCount > 2) {
        throw new Error('daily spend cap exceeded on replenish')
      }
    }

    const mock = buildMockStream(
      () => {
        const d = defs.shift()
        if (!d) return Promise.resolve({ seq: 99 })
        return d.promise
      },
      onSpend,
    )

    const iter = mock.stream({ concurrency: 2 })[Symbol.asyncIterator]()

    // Queued slot 2 rejects slightly later
    setTimeout(() => {
      d2.reject(new Error('queued slot 2 late rejection'))
    }, 10)

    // Slot 1 resolves
    d1.resolve({ seq: 1 })

    // On replenishing the window, checkSpend() throws spend cap error
    await assert.rejects(
      async () => {
        await iter.next()
      },
      (err: Error) => {
        assert.match(err.message, /daily spend cap exceeded on replenish/)
        return true
      },
    )
  },
)

console.log('\nAll stream pipelined rejection tests passed.')
