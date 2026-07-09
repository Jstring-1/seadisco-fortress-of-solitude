// ── Shared transient-retry helper for coverage workers ────────────
//
// Discogs occasionally 502s (upstream gateway drops) and 429s (rate
// limit); both are recoverable if the caller just backs off and
// retries the same request. The existing cache-warm-catno worker
// already treats 5xx/429 as transient — this file surfaces the same
// classifier plus a tiny retry wrapper so the four coverage workers
// don't have to hand-roll it.
export function isTransientDiscogsError(err) {
    const msg = String(err?.message ?? err ?? "");
    return /Discogs API error (5\d\d|429)/.test(msg);
}
const DEFAULT_BACKOFFS = [5_000, 15_000, 30_000];
const _sleep = (ms) => new Promise(r => setTimeout(r, ms));
// Retry `fn` on transient Discogs errors up to backoff.length times.
// Non-transient errors (400/404/etc.) throw immediately. Anything
// still failing after all backoffs is re-thrown to the caller so it
// can bump the worker's error counter.
export async function retryTransient(fn, opts = {}) {
    const backoff = opts.backoffMs ?? DEFAULT_BACKOFFS;
    let lastErr;
    for (let attempt = 0; attempt <= backoff.length; attempt++) {
        try {
            return await fn();
        }
        catch (err) {
            lastErr = err;
            if (!isTransientDiscogsError(err) || attempt === backoff.length)
                throw err;
            const waitMs = backoff[attempt];
            console.warn(`[${opts.label ?? "retry-transient"}] ${String(err?.message ?? err)} — sleeping ${waitMs}ms (attempt ${attempt + 1}/${backoff.length})`);
            await _sleep(waitMs);
        }
    }
    throw lastErr;
}
