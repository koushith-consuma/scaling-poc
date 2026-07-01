/**
 * Deterministic PRNG (mulberry32) + seed hashing.
 * Given the same seed, produces the same sequence — so load runs are reproducible.
 * When no seed is supplied, we fall back to Math.random via an entropy seed.
 */

export function hashSeed(...parts: (string | number)[]): number {
  // FNV-1a over the joined parts → 32-bit seed.
  let h = 0x811c9dc5;
  const s = parts.join('|');
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Build a rng: deterministic if seed given, else entropy-backed. */
export function makeRng(seed?: number): () => number {
  if (seed === undefined) {
    // Non-deterministic path: seed from a coarse entropy source.
    return mulberry32(hashSeed(String(process.hrtime.bigint()), String(process.pid)));
  }
  return mulberry32(seed >>> 0);
}

/** Uniform integer in [min, max] using the given rng. */
export function randInt(rng: () => number, min: number, max: number): number {
  return Math.floor(rng() * (max - min + 1)) + min;
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
