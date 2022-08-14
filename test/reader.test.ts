import { open } from '../reader.ts';
import {
  assertEquals,
  assertRejects,
} from 'https://deno.land/std@0.152.0/testing/asserts.ts';

Deno.test('Read from test1.cdb', async ctx => {
  const cdb = await open('./test/test1.cdb');

  await ctx.step('Get values of `one` using `Get`', async () => {
    const results = ['Hello', ', World!'];
    for await (const result of cdb.get('one')) {
      const text = new TextDecoder().decode(result);
      assertEquals(text, results.shift());
    }
  });

  await ctx.step('Get first value of `one`', async () => {
    const result = await cdb.getFirst('one');
    const text = new TextDecoder().decode(result);
    assertEquals(text, 'Hello');
  });

  await ctx.step('Get first value of long key', async () => {
    const result = await cdb.getFirst(
      'this key will be split across two reads'
    );
    const text = new TextDecoder().decode(result);
    assertEquals(text, 'Got it.');
  });

  await ctx.step('Get second value of `one`', async () => {
    const result = await cdb.getByOffset('one', 1);
    const text = new TextDecoder().decode(result);
    assertEquals(text, ', World!');
  });

  await ctx.step('Throw an error when there is no value', async () => {
    await assertRejects(async () => await cdb.getFirst('merong'));
    await assertRejects(async () => await cdb.getByOffset('two', 1));
  });

  cdb.close();
});
