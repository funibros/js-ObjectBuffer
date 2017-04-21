const bluebird = require('bluebird');
const levelup = require('levelup');
const AwaitLock = require('await-lock');

const key_rear = new Buffer([0, 0, 0, 0]);
const key_front = new Buffer([0, 0, 0, 1]);

class ObjectBuffer
{
    constructor(dbpath)
    {
        this.lock = new AwaitLock();
        this.buffer = [];
        this.db = levelup(dbpath, {
            compression: false,
            keyEncoding: 'binary',
            valueEncoding: 'json'
        });
        bluebird.promisifyAll(this.db);

        const initialize = async () => {
            await this.lock.acquireAsync();
            try {
                try {
                    const rear = await this.db.getAsync(key_rear);
                } catch(err) {
                    await this.db.putAsync(key_rear, 2);
                }
                try {
                    const front = await this.db.getAsync(key_front);
                } catch(err) {
                    await this.db.putAsync(key_front, 2);
                }
            } finally {
                this.lock.release();
            }
        };
        initialize();
    }

    push(o) {
        this.buffer.push(o);
    }

    async clear() {
        return this.remove(-1);
    }

    async remove(count) {
        await this.lock.acquireAsync();
        try {
            const meta = await Promise.all([this.db.getAsync(key_rear), this.db.getAsync(key_front)]);
            const rear = meta[0];
            let front = meta[1];
            if(count == -1) {
                count = rear - front;
            }
            count = Math.min(count, rear - front);
            const batch = this.db.batch();
            for(let i=0;i<count;i++) {
                const key = new Buffer(4); key.writeInt32BE(++front);
                batch.del(key);
            }
            if(front < rear) {
                batch.put(key_front, front);
            } else {
                batch.put(key_rear, 2);
                batch.put(key_front, 2);
            }
            return new Promise((resolve, reject) => {
                batch.write(err => {
                    if(err) {
                        reject(err);
                    }
                    resolve(count);
                })
            });
        } finally {
            this.lock.release();
        }
    }

    async peek(count) {
        await this.lock.acquireAsync();
        try {
            const meta = await Promise.all([this.db.getAsync(key_rear), this.db.getAsync(key_front)]);
            const rear = meta[0];
            const front = meta[1];
            count = Math.min(count, rear - front);
            const result = [];
            const startKey = new Buffer(4);
            startKey.writeInt32BE(front);
            return new Promise((resolve, reject) => {
                this.db.createReadStream({
                    keys: false,
                    values: true,
                    gte: startKey,
                    limit: count
                })
                .on('data', function (data) {
                    result.push(data);
                })
                .on('error', function (err) {
                    reject(err);
                })
                .on('close', function () {
                    resolve(result);
                })
                .on('end', function () {
                });
            });
        } finally {
            this.lock.release();
        }
    }

    async flush() {
        if(this.buffer.length == 0) {
            return;
        }

        await this.lock.acquireAsync();
        try {
            const buffer = this.buffer;
            this.buffer = [];
            let rear = await this.db.getAsync(key_rear);
            const batch = this.db.batch();
            for(const o of buffer) {
                const key = new Buffer(4); key.writeInt32BE(++rear);
                batch.put(key, o);
            }
            batch.put(key_rear, rear);
            return new Promise((resolve, reject) => {
                batch.write(err => {
                    if(err) {
                        reject(err);
                    }
                    resolve();
                })
            });
        } finally {
            this.lock.release();
        }
    }

    async count() {
        const meta = await Promise.all([this.db.getAsync(key_rear), this.db.getAsync(key_front)]);
        return meta[0] - meta[1];
    }
}

module.exports = ObjectBuffer;