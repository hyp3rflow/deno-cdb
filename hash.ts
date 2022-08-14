export function hash(k: string) {
  let h = 5381;
  for (const c of k) {
    h = ((((h << 5) >>> 0) + h) ^ c.charCodeAt(0)) >>> 0;
  }
  return h;
}
