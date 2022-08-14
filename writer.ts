import { hash } from './hash.ts';

export interface CDBWriter {
  set(key: string, value: Uint8Array): Promise<void>;
  finish(): Promise<void>;
}

export async function createWriter(path: string) {
  const file = await Deno.open(path, { write: true, create: true });
  return createWriterWithFsFile(file);
}

export async function createWriterWithFsFile(
  file: Deno.FsFile
): Promise<CDBWriter> {
  let pos = 2048;
  type Entry = { h: number; pos: number };
  const entries = new Map<number, Entry[]>();
  await file.seek(0, Deno.SeekMode.Start);
  await file.write(new Uint8Array(2048));
  return {
    async set(key: string, value: Uint8Array): Promise<void> {
      const buf = new Uint8Array(8);
      const view = new DataView(buf.buffer);
      const keyU8s = new TextEncoder().encode(key);
      view.setUint32(0, keyU8s.length, true);
      view.setUint32(4, value.length, true);
      await file.write(buf);
      await file.write(keyU8s);
      await file.write(value);
      const h = hash(key);
      entries.get(h % 256)?.push({ h, pos }) ??
        entries.set(h % 256, [{ h, pos }]);
      pos += 8 + keyU8s.length + value.length;
    },
    async finish() {
      const buf = new Uint8Array(8);
      const view = new DataView(buf.buffer);
      const headerU8s = new Uint8Array(2048);
      const headerView = new DataView(headerU8s.buffer);
      for (let i = 0; i < 256; i++) {
        const header = entries.get(i);
        if (!header) continue;
        const len = header.length * 2;
        headerView.setUint32(i * 8, pos, true);
        headerView.setUint32(i * 8 + 4, len, true);
        const table: Entry[] = [];
        for (const { h, pos } of header) {
          let wh = (h >>> 8) % len;
          while (table[wh]) {
            wh = ++wh === len ? 0 : wh;
          }
          table[wh] = { h, pos };
        }
        for (const hp of table) {
          const { h: pH, pos: pPos } = hp ?? { h: 0, pos: 0 };
          view.setUint32(0, pH, true);
          view.setUint32(4, pPos, true);
          await file.write(buf);
          pos += 8;
        }
      }
      await file.seek(0, Deno.SeekMode.Start);
      await file.write(headerU8s);
      file.close();
    },
  };
}
