import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import type { SupabaseClient } from '@supabase/supabase-js'
import { SupabaseSessionStore } from '../SessionStore.js'
import type { SessionState } from '../../types.js'
import { RouteDockVoucherMonotonicityError, RouteDockNetworkError } from '../../errors.js'

function mockSupabase(opts: {
  queryError?: string
  upsertError?: string
  updateError?: string
  data?: Record<string, unknown> | null
  onUpsert?: (payload: Record<string, unknown>, options?: unknown) => void
  onUpdate?: (payload: Record<string, unknown>, eqField?: string, eqVal?: string) => void
}): SupabaseClient {
  return {
    from(_table: string) {
      let updatePayload: Record<string, unknown> | null = null
      return {
        select(_cols: string) {
          return {
            eq(_field: string, _val: string) {
              return {
                maybeSingle() {
                  return Promise.resolve({
                    data: opts.data ?? null,
                    error: opts.queryError ? { message: opts.queryError } : null,
                  })
                },
              }
            },
          }
        },
        upsert(payload: Record<string, unknown>, options?: unknown) {
          opts.onUpsert?.(payload, options)
          return Promise.resolve({
            data: null,
            error: opts.upsertError ? { message: opts.upsertError } : null,
          })
        },
        update(payload: Record<string, unknown>) {
          updatePayload = payload
          return {
            eq(field: string, val: string) {
              opts.onUpdate?.(updatePayload!, field, val)
              return Promise.resolve({
                data: null,
                error: opts.updateError ? { message: opts.updateError } : null,
              })
            },
          }
        },
      }
    },
  } as unknown as SupabaseClient
}

const sampleRow = {
  channel_id: 'channel_123',
  payee: 'GPAYEE123',
  payer: 'GPAYER123',
  channel_contract: 'CCONTRACT123',
  network: 'testnet',
  cumulative_amount: '0.0050000',
  last_signature: 'sig_abc',
  status: 'open',
  opened_at: '2026-09-01T00:00:00.000Z',
  updated_at: '2026-09-01T00:00:00.000Z',
  settlement_tx_hash: null,
}

const sampleState: SessionState = {
  channel_id: 'channel_123',
  payee: 'GPAYEE123',
  payer: 'GPAYER123',
  channel_contract: 'CCONTRACT123',
  network: 'testnet',
  cumulative_amount: '0.0050000',
  last_signature: 'sig_abc',
  status: 'open',
  opened_at: '2026-09-01T00:00:00.000Z',
  updated_at: '2026-09-01T00:00:00.000Z',
  settlement_tx_hash: null,
}

describe('SupabaseSessionStore', () => {
  it('returns channel_contract and network from get()', async () => {
    const store = new SupabaseSessionStore(mockSupabase({ data: sampleRow }))
    const session = await store.get('channel_123')
    assert.ok(session)
    assert.equal(session.channel_contract, 'CCONTRACT123')
    assert.equal(session.network, 'testnet')
    assert.equal(session.channel_id, 'channel_123')
    assert.equal(session.payee, 'GPAYEE123')
    assert.equal(session.payer, 'GPAYER123')
  })

  it('upsert() sends non-null values for NOT NULL columns (channel_id, payee, payer, channel_contract) and network', async () => {
    let capturedPayload: Record<string, unknown> | null = null
    const store = new SupabaseSessionStore(
      mockSupabase({
        data: null,
        onUpsert: (payload) => {
          capturedPayload = payload
        },
      }),
    )

    await store.upsert('channel_123', sampleState)

    assert.ok(capturedPayload)
    assert.ok((capturedPayload as any).channel_id, 'channel_id must not be null/undefined')
    assert.ok((capturedPayload as any).payee, 'payee must not be null/undefined')
    assert.ok((capturedPayload as any).payer, 'payer must not be null/undefined')
    assert.ok((capturedPayload as any).channel_contract, 'channel_contract must not be null/undefined')
    assert.equal((capturedPayload as any).channel_contract, 'CCONTRACT123')
    assert.equal((capturedPayload as any).network, 'testnet')
    assert.equal((capturedPayload as any).channel_id, 'channel_123')
  })

  it('setStatus() calls update with a payload that has no cumulative_amount key', async () => {
    let capturedPayload: Record<string, unknown> | null = null
    let capturedEqField: string | null = null
    let capturedEqVal: string | null = null
    const store = new SupabaseSessionStore(
      mockSupabase({
        onUpdate: (payload, eqField, eqVal) => {
          capturedPayload = payload
          capturedEqField = eqField ?? null
          capturedEqVal = eqVal ?? null
        },
      }),
    )

    await store.setStatus('channel_123', 'closing')

    assert.ok(capturedPayload)
    assert.equal('cumulative_amount' in capturedPayload, false)
    assert.equal((capturedPayload as any).status, 'closing')
    assert.ok((capturedPayload as any).updated_at)
    assert.equal(capturedEqField, 'channel_id')
    assert.equal(capturedEqVal, 'channel_123')
  })

  it('moving a stored session from open to closing at an unchanged amount resolves without throwing', async () => {
    let capturedPayload: Record<string, unknown> | null = null
    const store = new SupabaseSessionStore(
      mockSupabase({
        data: sampleRow,
        onUpdate: (payload) => {
          capturedPayload = payload
        },
      }),
    )

    await assert.doesNotReject(async () => {
      await store.setStatus('channel_123', 'closing')
    })
    assert.equal((capturedPayload as any)?.status, 'closing')
    assert.equal('cumulative_amount' in (capturedPayload ?? {}), false)
  })

  it('setStatus() writes settlement_tx_hash when one is passed', async () => {
    let capturedPayload: Record<string, unknown> | null = null
    const store = new SupabaseSessionStore(
      mockSupabase({
        onUpdate: (payload) => {
          capturedPayload = payload
        },
      }),
    )

    await store.setStatus('channel_123', 'closed', 'tx_settled_hash_999')

    assert.ok(capturedPayload)
    assert.equal((capturedPayload as any).status, 'closed')
    assert.equal((capturedPayload as any).settlement_tx_hash, 'tx_settled_hash_999')
    assert.equal('cumulative_amount' in capturedPayload, false)
  })

  it('close() sends status: closed and no cumulative_amount', async () => {
    let capturedPayload: Record<string, unknown> | null = null
    const store = new SupabaseSessionStore(
      mockSupabase({
        onUpdate: (payload) => {
          capturedPayload = payload
        },
      }),
    )

    await store.close('channel_123')

    assert.ok(capturedPayload)
    assert.equal((capturedPayload as any).status, 'closed')
    assert.equal('cumulative_amount' in capturedPayload, false)
  })

  it('upsert() rejects with RouteDockVoucherMonotonicityError when cumulative_amount <= stored, without calling client upsert', async () => {
    let upsertCalled = false
    const store = new SupabaseSessionStore(
      mockSupabase({
        data: sampleRow, // cumulative_amount is 0.0050000
        onUpsert: () => {
          upsertCalled = true
        },
      }),
    )

    // Equal amount
    await assert.rejects(
      () =>
        store.upsert('channel_123', {
          ...sampleState,
          cumulative_amount: '0.0050000',
        }),
      (err: Error) => {
        assert.equal(err.name, 'RouteDockVoucherMonotonicityError')
        return true
      },
    )
    assert.equal(upsertCalled, false)

    // Lower amount
    await assert.rejects(
      () =>
        store.upsert('channel_123', {
          ...sampleState,
          cumulative_amount: '0.0040000',
        }),
      (err: Error) => {
        assert.equal(err.name, 'RouteDockVoucherMonotonicityError')
        return true
      },
    )
    assert.equal(upsertCalled, false)
  })
})
