const ObjectBuffer = require('level-object-buffer');

async function example() {
    // 1) Create ObjectBuffer with LevelDB Database
    const buffer = new ObjectBuffer('./_mybuffer');

    // 2) Put objects on memory
    buffer.push({ name: 'object1', index: 1 });
    buffer.push({ name: 'object2', index: 2 });
    buffer.push({ name: 'object3', index: 3 });
    buffer.push({ name: 'object4', index: 4 });

    // 3) Flush to database
    await buffer.flush();

    // 4) Check buffer size
    const count = await buffer.count();

    // 5) Fetch 3 objects.
    const res = await buffer.peek(3);
    console.log(res);

    // 6) Remove 2 items.
    await buffer.remove(2);
    console.log(await buffer.count()); // 2 item left.

    // 7) Remove all items.
    await buffer.clear();
    console.log(await buffer.count()); // 0 no more item.
}

example();