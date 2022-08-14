import { open } from '../reader.ts';
import { assertEquals } from 'https://deno.land/std@0.152.0/testing/asserts.ts';

Deno.test('Read one from test1.cdb', async () => {
  const cdb = await open('./test/test1.cdb');
  const result = await cdb.getFirst('one');
  const text = new TextDecoder().decode(result);
  assertEquals(text, 'Hello');
  cdb.close();
});
