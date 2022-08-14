import { hash } from './hash.ts';

export interface CDB extends Deno.Closer {
  get(key: string): AsyncGenerator<Uint8Array>;
  getFirst(key: string): Promise<Uint8Array>;
  getByOffset(key: string, offset: number): Promise<Uint8Array>;
  find(key: string): string[];
}

export async function open(path: string): Promise<CDB> {
  const file = await Deno.open(path, { read: true });
  const stat = await file.stat();
  if (stat.size < 2048 || stat.size > 0xffff_ffff) {
    throw new Deno.errors.InvalidData('Invalid file format');
  }
  return await create(file);
}

export async function create(file: Deno.FsFile): Promise<CDB> {
  return {
    close: file.close.bind(file),
    async *get(key: string): AsyncGenerator<Uint8Array> {
      yield* getValueGenerator(key);
    },
    async getFirst(key: string): Promise<Uint8Array> {
      return await elementAt(getValueGenerator(key), 0);
    },
    async getByOffset(key: string, offset: number): Promise<Uint8Array> {
      return await elementAt(getValueGenerator(key), offset);
    },
    find(key: string): string[] {
      throw new Error('TODO');
    },
  };
  async function* getValueGenerator(key: string): AsyncGenerator<Uint8Array> {
    const h = hash(key);
    const [p, e] = await getHashEntry(h);
    if (!e) return;
    for await (const [, rp] of genRecord(h, p, e, key)) {
      const { data } = await getRecord(rp);
      yield data;
    }
  }
  /**
   * Get hash entry from the main table in the CDB file.
   */
  async function getHashEntry(hash: number) {
    const x = hash % 256 << 3;
    await file.seek(x, Deno.SeekMode.Start);
    const buf = new Uint8Array(8);
    await file.read(buf);
    const view = new DataView(buf.buffer);
    const pointer = view.getUint16(0, true);
    const count = view.getUint16(4, true);
    return [pointer, count];
  }
  /**
   * Find the correct hashpair in Subtable of the CDB file.
   */
  async function* genRecord(
    hash: number,
    pointer: number,
    count: number,
    key: string
  ): AsyncGenerator<[number, number]> {
    const start = (hash >>> 8) % count;
    for (let i = 0; i < count; i++) {
      const offset = pointer + 8 * ((start + i) % count);
      const buf = new Uint8Array(8);
      await file.seek(offset, Deno.SeekMode.Start);
      await file.read(buf);
      const view = new DataView(buf.buffer);
      const h = view.getUint32(0, true);
      const p = view.getUint32(4, true);
      if (p === 0) return;
      if (h !== hash) continue;
      // to prevent the hash collision
      const len = await getRecordKeyLength(p);
      if (key.length !== len) continue;
      const rKey = await getRecordKey(p, len);
      if (key !== rKey) continue;
      yield [h, p];
    }
    return;
    async function getRecordKey(pointer: number, len: number) {
      const buf = new Uint8Array(len);
      await file.seek(pointer + 8, Deno.SeekMode.Start);
      await file.read(buf);
      return new TextDecoder().decode(buf);
    }
    async function getRecordKeyLength(pointer: number) {
      const buf = new Uint8Array(4);
      await file.seek(pointer, Deno.SeekMode.Start);
      await file.read(buf);
      return new DataView(buf.buffer).getUint32(0, true);
    }
  }
  async function getRecord(pointer: number) {
    const buf = new Uint8Array(8);
    await file.seek(pointer, Deno.SeekMode.Start);
    await file.read(buf);
    const view = new DataView(buf.buffer);
    const keyLen = view.getUint32(0, true);
    const dataLen = view.getUint32(4, true);
    const key = new Uint8Array(keyLen);
    const data = new Uint8Array(dataLen);
    await file.read(key);
    await file.read(data);
    return { key, data };
  }
}

export async function elementAt<T>(
  gen: AsyncGenerator<T>,
  offset: number
): Promise<T> {
  for (let i = 0; i < offset; i++) {
    const { done } = await gen.next();
    if (done) break;
  }
  const { done, value } = await gen.next();
  if (done) throw new Error('Offset out of range');
  return value;
}
