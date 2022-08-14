import { assertEquals, assertRejects } from 'https://deno.land/std@0.152.0/testing/asserts.ts';
import { open } from '../reader.ts';
import { createWriter } from '../writer.ts';

Deno.test('Write some CDB file', async () => {
  const writer = await createWriter('test/test2.cdb');
  await writer.set('one', new TextEncoder().encode('Hello'));
  await writer.set('two', new TextEncoder().encode('Goodbye'));
  await writer.set('one', new TextEncoder().encode(', World!'));
  await writer.set(
    'this key will be split across two reads',
    new TextEncoder().encode('Got it.')
  );
  await writer.finish();
});

Deno.test('Read written file', async ctx => {
  const cdb = await open('test/test2.cdb');

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
