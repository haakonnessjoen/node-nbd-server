const net = require('net');
const Redis = require('ioredis');
const redis = new Redis();

const INIT_PASSWD = "NBDMAGIC";
const OPTS_MAGIC = "IHAVEOPT";
const MAGIC = Buffer.from("0003e889045565a9", "hex");

const NBD_REQUEST_MAGIC = 0x25609513;
const NBD_REPLY_MAGIC = 0x67446698;
const NBD_STRUCTURED_REPLY_MAGIC = 0x668e33ef;

const NBD_CMD_MASK_COMMAND = 0xffff;

const NBD_CMD_READ = 0;
const NBD_CMD_WRITE = 1;
const NBD_CMD_DISC = 2;
const NBD_CMD_FLUSH = 3;
const NBD_CMD_TRIM = 4;
const NBD_CMD_CACHE = 5;
const NBD_CMD_WRITE_ZEROES = 6;
const NBD_CMD_BLOCK_STATUS = 7;
const NBD_CMD_RESIZE = 8;

const NBD_FLAG_FIXED_NEWSTYLE = (1 << 0);
const NBD_FLAG_NO_ZEROES = (1 << 1);

const NBD_FLAG_HAS_FLAGS         = (1 << 0);
const NBD_FLAG_READ_ONLY         = (1 << 1);
const NBD_FLAG_SEND_FLUSH        = (1 << 2);
const NBD_FLAG_SEND_FUA          = (1 << 3);
const NBD_FLAG_ROTATIONAL        = (1 << 4);
const NBD_FLAG_SEND_TRIM         = (1 << 5);
const NBD_FLAG_SEND_WRITE_ZEROES = (1 << 6);
const NBD_FLAG_CAN_MULTI_CONN    = (1 << 8);

const NBD_INFO_EXPORT     = 0;

const NBD_OPT_EXPORT_NAME = 1;	/**< Client wants to select a named export (is followed by name of export) */
const NBD_OPT_ABORT       = 2;	/**< Client wishes to abort negotiation */
const NBD_OPT_LIST        = 3;	/**< Client request list of supported exports (not followed by data) */
const NBD_OPT_STARTTLS    = 5;	/**< Client wishes to initiate TLS */
const NBD_OPT_INFO        = 6;	/**< Client wants information about the given export */
const NBD_OPT_GO          = 7;

const NBD_REP_ACK         = 1;
const NBD_REP_INFO        = 3;

const NBD_REP_FLAG_ERROR          = (1 << 31);
const NBD_REP_ERR_UNSUP           = (1 | NBD_REP_FLAG_ERROR);
const NBD_REP_ERR_POLICY          = (2 | NBD_REP_FLAG_ERROR);
const NBD_REP_ERR_INVALID         = (3 | NBD_REP_FLAG_ERROR);
const NBD_REP_ERR_PLATFORM        = (4 | NBD_REP_FLAG_ERROR);
const NBD_REP_ERR_TLS_REQD        = (5 | NBD_REP_FLAG_ERROR);
const NBD_REP_ERR_UNKNOWN         = (6 | NBD_REP_FLAG_ERROR);
const NBD_REP_ERR_BLOCK_SIZE_REQD = (8 | NBD_REP_FLAG_ERROR);

const NBD_REP_ERRNO_OK = 0;
const NBD_REP_ERRNO_EPERM = 1;
const NBD_REP_ERRNO_EIO = 5;
const NBD_REP_ERRNO_ENOMEM = 12;
const NBD_REP_ERRNO_EINVAL = 22;
const NBD_REP_ERRNO_EFBIG = 28; // ENOSPC
const NBD_REP_ERRNO_ENOSPC = 28;
const NBD_REP_ERRNO_EDQUOT = 28; // ENOSPC

const fileSystemSize = 106954752; //1073741824n; // 1G

/**
 * TODO: Lag work-queue for inngående pakker.
 * Virker som inputbufferet fra unix socketen går full i node(?)
 * fordi vi går plutselig ut av sync når det skrives data fort.
 * Så isteden for å vente på redis, la oss bare fylle en kø med kommandoer,
 * siden operativsystemet uansett i teorien ikke sender mer enn x samtidige requests.
 * Så det er kun issue med store buffere, ikke antall ventende operasjoner.
 */

function send_reply(socket, opt, type, buf) {
	const buffer = Buffer.alloc(20 + (buf?.length || 0));
	MAGIC.copy(buffer);
	buffer.writeUInt32BE(opt, 8);
	buffer.writeInt32BE(type, 8+4);
	buffer.writeUint32BE(buf?.length || 0, 8+4+4);
	if (buf?.length) {
		buf.copy(buffer, 8+4+4+4);
	}

	console.log("Sending ", buffer, buffer.toString());
	return new Promise((resolve) => socket.write(buffer, resolve));
}

function data_reply(socket, errno, handle) {
	/*struct nbd_reply {
		uint32_t magic;
		uint32_t error;		// 0 = ok, else error
		char handle[8];		// handle you got from request
	};*/
	const buffer = Buffer.alloc(16);
	buffer.writeUInt32BE(NBD_REPLY_MAGIC);
	buffer.writeUInt32BE(errno, 4);
	handle.copy(buffer,8);

	return new Promise((resolve) => socket.write(buffer, resolve));
}

redis.getBuffer('device0:0').then((data)=>{console.log("redis works:", data);}).catch(e => console.error('Redis error. ', e));


const serv = net.createServer((socket) => {
	let fullbuffer = Buffer.alloc(0);
	let state = 0;
	console.log('client');

	socket.on('error', (e) => {
		console.log("SOCKET ERROR::::: ", e);
	})

	socket.write(INIT_PASSWD);

	socket.write("IHAVEOPT");

	const flags = Buffer.from([0,0]);
	flags.writeUint16BE(NBD_FLAG_FIXED_NEWSTYLE);
	socket.write(flags);

	let ready = true;

	socket.on("data", async (indata) => {
		if (!ready) {
			await new Promise(resolve => { let tim = setInterval(() => { if (ready) { clearInterval(tim); resolve() } }, 50)});
		}
		fullbuffer = data = Buffer.concat([fullbuffer, indata]);
		let offset = 0;

		ready = false;
		do {
			if (fullbuffer.length < 12) { ready = true; return; }
			offset = 0;

			if (state === 0) {
				const flags = data.readUint32BE(0);
				console.log("Flags: ", flags);
				const IHAVEOPT = data.subarray(4, 12).toString();
				if (!(flags & NBD_FLAG_FIXED_NEWSTYLE) || IHAVEOPT !== OPTS_MAGIC) {
					console.log("Invalid reply from client, flags/magic mismatch");
					socket.end();
					ready = true;
					return;
				}
				offset += 12;

				while (offset + 12 < data.length) {
					const opts = data.readUint32BE(offset); offset += 4;
					const optlen = data.readUInt32BE(offset); offset += 4;

					if (opts === NBD_OPT_GO || opts === NBD_OPT_INFO) {
						let sentInfo = false;
						const namelen = data.readUInt32BE(offset); offset += 4;
						const name = data.subarray(offset, offset + namelen).toString();
						console.log("Acking GO/INFO");
						offset += namelen;

						// TODO, loop through export requests or something
						const requests = data.readUint16BE(offset); offset += 2;
						console.log("Requests: ", requests)

						if (name !== 'device0') {
							await send_reply(socket, opts, NBD_REP_ERR_UNKNOWN, Buffer.from("Export unknown"));
							break;
						}

						if (!sentInfo) {
							const result = Buffer.alloc(12);
							result.writeUint16BE(NBD_INFO_EXPORT);

							const size = fileSystemSize;
							result.writeBigUInt64BE(BigInt(size), 2);

							const flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_WRITE_ZEROES;
							result.writeUInt16BE(flags, 10);

							console.log("result: ", result);
							await send_reply(socket, opts, NBD_REP_INFO, result);
						}

						await send_reply(socket, opts, NBD_REP_ACK);
					} else {
						await send_reply(socket, opts, NBD_REP_ERR_UNSUP);
						console.log("Unhandeled opt " + opts);
						offset += optlen;
					}

					console.log("next, offset is: ", offset);
				}
				console.log("done", offset, data.length);
				state = 1;
			} else {
				/*
					struct nbd_request {
						uint32_t magic;
						uint32_t type;	// == READ || == WRITE
						char handle[8];
						uint64_t from;
						uint32_t len;
					} __attribute__ ((packed));
				*/
				console.log("DATA REQUEST:", data);
				const magic = data.readUInt32BE(offset); offset += 4;
				if (magic !== NBD_REQUEST_MAGIC) {
					console.log("Unknown magic: ", magic.toString(16));
					// Out of sync, reset the buffer
					fullbuffer = Buffer.alloc(0);
					ready = true;
					return;
				}

				const requestType = data.readUInt32BE(offset); offset += 4;
				const handle = Buffer.from(data.subarray(offset, offset + 8)); offset += 8;
				const from = data.readBigUInt64BE(offset); offset += 8;
				const len = data.readUint32BE(offset); offset += 4;

				const cmd = (requestType & NBD_CMD_MASK_COMMAND);

				switch (cmd) {
					case NBD_CMD_WRITE:
						console.log(handle.toString('hex') + "] Write request, from " + from + ' Size: ' + len)
						try {
							if (offset + len > fullbuffer.length) {
								// Missing data, wait for more data
								ready = true;
								return;
							}
							await redis.set('device0:' + (from / 4096n), data.subarray(offset, offset + len));
							offset += len;
							await data_reply(socket, NBD_REP_ERRNO_OK, handle);
						} catch (e) {
							console.log("NBD_CMD_WRITE: ", e);
							await data_reply(socket, NBD_REP_ERRNO_EIO, handle);
						}
						break;

					case NBD_CMD_READ:
						console.log(handle.toString('hex') + "] Read request, from " + from + ' Size: ' + len)
						const outbuff = Buffer.alloc(len);
						const errors = 0;

						try {
							if (from > fileSystemSize || from + BigInt(len) > fileSystemSize) {
								await data_reply(socket, NBD_REP_ERRNO_EINVAL, handle);
								console.log(handle.toString('hex') + "] INVAL");
								break;
							}
							for (let i = 0; i < len; i += 4096) {
								const fromredis = await redis.getBuffer('device0:' + ((from + BigInt(i)) / 4096n));
								if (fromredis instanceof Buffer) {
									if (from % 4096n !== 0) {
										console.log("oops, reading between buffers, not supported :(");
									}
									fromredis.copy(outbuff, i);
								} else {
									let notfromredis = Buffer.alloc(4096);
									notfromredis.copy(outbuff, Math.min(i, len - 4096));
								}
							}
							console.log("redis: ", outbuff);

							await data_reply(socket, NBD_REP_ERRNO_OK, handle);
							console.log(handle.toString('hex') + "] SEND-> ", outbuff.length, outbuff)
							await new Promise((resolve) => socket.write(outbuff, resolve));
						} catch (e) {
							console.log(handle.toString('hex') + "] NBD_CMD_READ: EIO: ", e);
							await data_reply(socket, NBD_REP_ERRNO_EIO, handle);
						}
						break;
					case NBD_CMD_FLUSH:
						console.log(handle.toString('hex') + "] FLUSH request, from " + from + ' Size: ' + len)
						await data_reply(socket, NBD_REP_ERRNO_OK, handle);
						break;
					case NBD_CMD_TRIM:
						console.log(handle.toString('hex') + "] TRIM request, from " + from + ' Size: ' + len)
						await data_reply(socket, NBD_REP_ERRNO_OK, handle);
						break;
					case NBD_CMD_WRITE_ZEROES:
						console.log(handle.toString('hex') + "] WRITE_ZEROES request, from " + from + ' Size: ' + len)

						try {
							for (let i = 0; i < len; i += 4096) {
								await redis.del('device0:' + ((from + BigInt(i)) / 4096n));
							}
							await data_reply(socket, NBD_REP_ERRNO_OK, handle);
						} catch (e) {
							console.log("NBD_CMD_WRITE_ZEROES: EIO: ", e);
							await data_reply(socket, NBD_REP_ERRNO_EIO, handle);
						}
						break;
					default:
						await data_reply(socket, NBD_REP_ERRNO_EINVAL, handle);
				}
			}

			data = fullbuffer = Buffer.from(fullbuffer.subarray(offset));
		} while (offset > 0);
		ready = true;
	});
});

serv.listen('/tmp/nbdev.sock');