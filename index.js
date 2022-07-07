const net = require("net");
const Redis = require("ioredis");
const redis = new Redis();

const INIT_PASSWD = "NBDMAGIC";
const OPTS_MAGIC = "IHAVEOPT";
const MAGIC = Buffer.from("0003e889045565a9", "hex");

const NBD_REQUEST_MAGIC = 0x25609513;
const NBD_REPLY_MAGIC = 0x67446698;
const NBD_STRUCTURED_REPLY_MAGIC = 0x668e33ef;

const NBD_CMD_MASK_COMMAND = 0xffff;
const NBD_CMD_SHIFT = 16;
const NBD_CMD_FLAG_FUA = (1 << 0) << NBD_CMD_SHIFT;
const NBD_CMD_FLAG_NO_HOLE = (1 << 1) << NBD_CMD_SHIFT;

const NBD_CMD_READ = 0;
const NBD_CMD_WRITE = 1;
const NBD_CMD_DISC = 2;
const NBD_CMD_FLUSH = 3;
const NBD_CMD_TRIM = 4;
const NBD_CMD_CACHE = 5;
const NBD_CMD_WRITE_ZEROES = 6;
const NBD_CMD_BLOCK_STATUS = 7;
const NBD_CMD_RESIZE = 8;

const NBD_FLAG_FIXED_NEWSTYLE = 1 << 0;
const NBD_FLAG_NO_ZEROES = 1 << 1;

const NBD_FLAG_HAS_FLAGS = 1 << 0;
const NBD_FLAG_READ_ONLY = 1 << 1;
const NBD_FLAG_SEND_FLUSH = 1 << 2;
const NBD_FLAG_SEND_FUA = 1 << 3;
const NBD_FLAG_ROTATIONAL = 1 << 4;
const NBD_FLAG_SEND_TRIM = 1 << 5;
const NBD_FLAG_SEND_WRITE_ZEROES = 1 << 6;
const NBD_FLAG_CAN_MULTI_CONN = 1 << 8;

const NBD_INFO_EXPORT = 0;

const NBD_OPT_EXPORT_NAME = 1; /**< Client wants to select a named export (is followed by name of export) */
const NBD_OPT_ABORT = 2; /**< Client wishes to abort negotiation */
const NBD_OPT_LIST = 3; /**< Client request list of supported exports (not followed by data) */
const NBD_OPT_STARTTLS = 5; /**< Client wishes to initiate TLS */
const NBD_OPT_INFO = 6; /**< Client wants information about the given export */
const NBD_OPT_GO = 7;

const NBD_REP_ACK = 1;
const NBD_REP_INFO = 3;

const NBD_REP_FLAG_ERROR = 1 << 31;
const NBD_REP_ERR_UNSUP = 1 | NBD_REP_FLAG_ERROR;
const NBD_REP_ERR_POLICY = 2 | NBD_REP_FLAG_ERROR;
const NBD_REP_ERR_INVALID = 3 | NBD_REP_FLAG_ERROR;
const NBD_REP_ERR_PLATFORM = 4 | NBD_REP_FLAG_ERROR;
const NBD_REP_ERR_TLS_REQD = 5 | NBD_REP_FLAG_ERROR;
const NBD_REP_ERR_UNKNOWN = 6 | NBD_REP_FLAG_ERROR;
const NBD_REP_ERR_BLOCK_SIZE_REQD = 8 | NBD_REP_FLAG_ERROR;

const NBD_REP_ERRNO_OK = 0;
const NBD_REP_ERRNO_EPERM = 1;
const NBD_REP_ERRNO_EIO = 5;
const NBD_REP_ERRNO_ENOMEM = 12;
const NBD_REP_ERRNO_EINVAL = 22;
const NBD_REP_ERRNO_EFBIG = 28; // ENOSPC
const NBD_REP_ERRNO_ENOSPC = 28;
const NBD_REP_ERRNO_EDQUOT = 28; // ENOSPC

// TODO: implement non-block aligned read/writes
// TODO: Find out why full speed writes sometimes fail after 4-500MB going out of sync

const STATE_INIT = 0;
const STATE_NORMAL = 1;

class NBDExports {
	static exports = {};

	static add(name, info) {
		NBDExports.exports[name] = info; // { name, size }
	}

	static get(name) {
		return NBDExports.exports[name];
	}
}

class NBDServer {
	unloadFunction = () => {};
	connected = true;
	workload = [];
	fullbuffer = Buffer.alloc(0);

	constructor(socket, unloadFunction) {
		this.socket = socket;
		this.state = STATE_INIT;

		if (unloadFunction && typeof unloadFunction === "function") {
			this.unloadFunction = unloadFunction;
		}

		console.log("connection");
		this.socket.on("data", (data) => {
			this.handleData(data);
		});
		this.socket.on("error", (err) => {
			this.unload();
			console.log("SOCKERR:", err);
		});
		this.socket.on("close", () => {
			this.unload();
			console.log("close");
		});
		/*
		this.timer = setInterval(() => {
			console.log("Workload: ", this.workload);
		}, 2000)*/
		this.timer = setInterval(() => {
			this.checkWorkload();
		}, 500);

		this.initSocket();
	}

	socketWrite(buffer) {
		if (this.connected) {
			return new Promise((resolve, reject) => {
				this.socket.write(buffer, (err) => {
					if (err) {
						console.log("socketWrite error:", err);
						this.unload();
					}

					resolve();
				});
			});
		}
	}

	sendReply(opt, type, buf) {
		const buffer = Buffer.alloc(20 + (buf?.length || 0));

		MAGIC.copy(buffer);
		buffer.writeUInt32BE(opt, 8);
		buffer.writeInt32BE(type, 8 + 4);
		buffer.writeUint32BE(buf?.length || 0, 8 + 4 + 4);
		if (buf?.length) {
			buf.copy(buffer, 8 + 4 + 4 + 4);
		}

		console.log("NEGOTIATE_REPLY> ", buffer, buffer.toString());

		return this.socketWrite(buffer);
	}

	dataReply(errno, handle, buf) {
		/*struct nbd_reply {
			uint32_t magic;
			uint32_t error;		// 0 = ok, else error
			char handle[8];		// handle you got from request
		};*/
		const buffer = Buffer.alloc(16 + (buf?.length || 0));
		buffer.writeUInt32BE(NBD_REPLY_MAGIC);
		buffer.writeUInt32BE(errno, 4);
		handle.copy(buffer, 8, 0, 8);
		if (buf?.length) {
			buf.copy(buffer, 16);
		}

		console.log(
			handle.toString("hex") +
				"] REPLY " +
				errno +
				" (" +
				(buf?.length || 0) +
				" bytes)"
		);

		return this.socketWrite(buffer);
	}

	initSocket() {
		this.socket.write(INIT_PASSWD);

		this.socket.write("IHAVEOPT");

		const flags = Buffer.alloc(2);
		flags.writeUint16BE(NBD_FLAG_FIXED_NEWSTYLE);

		this.socket.write(flags);
	}

	handleData(data) {
		let count = 0;
		this.fullbuffer = Buffer.concat([this.fullbuffer, data]);

		while (this.fullbuffer.length > 12) {
			if (this.state === STATE_INIT) {
				const offset = this.handleNegotiatePacket();
				if (offset > 0) {
					this.fullbuffer = this.fullbuffer.subarray(offset);
				} else {
					this.checkWorkload();
					return;
				}
			} else if (this.state === STATE_NORMAL && this.fullbuffer.length >= 28) {
				const offset = this.handleNormalPacket();
				if (offset > 0) {
					this.fullbuffer = this.fullbuffer.subarray(offset);
				} else {
					this.checkWorkload();
					return;
				}
			}

			// Anti lock up
			if (count++ > 100) {
				console.log("Seem to be out of sync, bailing", count);
				this.fullbuffer = Buffer.alloc(0);
				break;
			}
		}

		this.checkWorkload();
	}

	async checkWorkload() {
		while (this.workload.length) {
			let work = this.workload.shift();

			if (work.state === STATE_INIT) {
				const opts = work.opts;

				if (opts === NBD_OPT_GO || opts === NBD_OPT_INFO) {
					let sentInfo = false;
					const name = work.name;

					console.log("Acking GO/INFO");

					const nbdexport = NBDExports.get(name);
					if (nbdexport === undefined) {
						await this.sendReply(
							opts,
							NBD_REP_ERR_UNKNOWN,
							Buffer.from("Export unknown")
						);
						break;
					}

					this.size = nbdexport.size;
					this.name = name;

					// TODO: Handle requests

					if (!sentInfo) {
						const result = Buffer.alloc(12);
						result.writeUint16BE(NBD_INFO_EXPORT);

						result.writeBigUInt64BE(nbdexport.size, 2);

						const flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_WRITE_ZEROES;
						result.writeUInt16BE(flags, 10);

						console.log("info: ", result);
						await this.sendReply(opts, NBD_REP_INFO, result);
					}

					await this.sendReply(opts, NBD_REP_ACK);
				} else {
					await this.sendReply(opts, NBD_REP_ERR_UNSUP);
					console.log("Unhandeled opt " + opts);
				}
			} else {
				if (work.flags & ~(NBD_CMD_FLAG_FUA | NBD_CMD_FLAG_NO_HOLE)) {
					console.log(
						"Invalid flag %o on command %o, ignoring",
						work.flags,
						work.cmd
					);
					await this.dataReply(NBD_REP_ERRNO_EIO, work.handle);
					break;
				}

				switch (work.cmd) {
					case NBD_CMD_WRITE:
						try {
							if (
								work.from > this.size ||
								work.from + BigInt(work.len) > this.size
							) {
								await this.dataReply(NBD_REP_ERRNO_ENOSPC, work.handle);
								break;
							}

							const promises = [];
							for (let i = 0; i < work.len; i += 4096) {
								const index = this.name + ":" + (work.from + BigInt(i)) / 4096n;
								let subarr = work.data.subarray(i, i + 4096);
								if (subarr.length < 4096) {
									subarr = Buffer.concat([
										subarr,
										Buffer.alloc(4096 - subarr.length),
									]);
								}

								// Auto memory cleanup
								if ([...subarr].reduce((s, a) => s + a, 0) === 0) {
									console.log(
										work.handle.toString("hex") + "] CLEARING " + index
									);
									promises.push(redis.del(index));
								} else {
									promises.push(redis.set(index, subarr));
								}
							}
							await Promise.all(promises);

							console.log(
								work.handle.toString("hex") +
									"] WRITE " +
									work.from +
									" : " +
									work.len +
									" bytes"
							);
							await this.dataReply(NBD_REP_ERRNO_OK, work.handle);
						} catch (e) {
							console.log("NBD_CMD_WRITE: ", e);
							await this.dataReply(NBD_REP_ERRNO_EIO, work.handle);
						}
						break;

					case NBD_CMD_READ:
						try {
							if (
								work.from > this.size ||
								work.from + BigInt(work.len) > this.size
							) {
								await this.dataReply(NBD_REP_ERRNO_EINVAL, work.handle);
								break;
							}

							const promises = [];
							for (let i = 0; i < work.len; i += 4096) {
								const index = this.name + ":" + (work.from + BigInt(i)) / 4096n;
								promises.push(redis.getBuffer(index));
							}
							const data = await Promise.all(promises);
							let isBuffers = true;
							data.forEach((d) => {
								if (!(d instanceof Buffer)) isBuffers = false;
							});

							console.log(
								work.handle.toString("hex") +
									"] READ " +
									work.from +
									" : " +
									work.len +
									" bytes"
							);

							if (isBuffers) {
								let result = Buffer.concat(data);

								if (result.length < work.len) {
									console.log(
										"ERROR: Short read: ",
										result.length,
										"vs",
										work.len
									);
									result = Buffer.alloc(work.len);
								}

								if (result.length > work.len) {
									result = result.subarray(0, work.len);
								}

								await this.dataReply(NBD_REP_ERRNO_OK, work.handle, result);
							} else {
								const result = Buffer.alloc(work.len);
								await this.dataReply(NBD_REP_ERRNO_OK, work.handle, result);
								console.log(
									work.handle.toString("hex") +
										"] Sending " +
										result.length +
										" nullbytes"
								);
							}
						} catch (e) {
							console.log("NBD_CMD_READ: ", e);
							await this.dataReply(NBD_REP_ERRNO_EIO, work.handle);
						}
						break;

					case NBD_CMD_FLUSH:
						console.log(work.handle.toString("hex") + "] FLUSH request");
						await this.dataReply(NBD_REP_ERRNO_OK, work.handle);
						break;

					case NBD_CMD_TRIM:
						console.log(work.handle.toString("hex") + "] TRIM request");
						await this.dataReply(NBD_REP_ERRNO_OK, work.handle);
						break;

					case NBD_CMD_WRITE_ZEROES:
						try {
							const promises = [];
							for (let i = 0; i < len; i += 4096) {
								const index = this.name + ":" + (work.from + BigInt(i)) / 4096n;
								promises.push(redis.del(index));
							}
							await Promise.all(promises);
							await this.dataReply(NBD_REP_ERRNO_OK, handle);
						} catch (e) {
							console.log("NBD_CMD_WRITE_ZEROES: EIO: ", e);
							await this.dataReply(NBD_REP_ERRNO_EIO, handle);
						}
						break;
				}
			}
		}
	}

	handleNegotiatePacket() {
		const data = this.fullbuffer;
		let offset = 0;

		const flags = data.readUint32BE(0);
		console.log("Flags: ", flags);
		const IHAVEOPT = data.subarray(4, 12).toString();

		if (!(flags & NBD_FLAG_FIXED_NEWSTYLE) || IHAVEOPT !== OPTS_MAGIC) {
			console.log("Invalid reply from client, flags/magic mismatch");
			this.socket.end();
			this.unload();
			return 0;
		}
		offset += 12;

		while (offset + 12 < data.length) {
			const opts = data.readUint32BE(offset);
			offset += 4;
			const optlen = data.readUInt32BE(offset);
			offset += 4;

			if (opts === NBD_OPT_GO || opts === NBD_OPT_INFO) {
				let sentInfo = false;
				const namelen = data.readUInt32BE(offset);
				offset += 4;
				const name = data.subarray(offset, offset + namelen).toString();
				console.log("Acking GO/INFO");
				offset += namelen;

				// TODO, loop through export requests or something
				const requests = data.readUint16BE(offset);
				offset += 2;
				console.log("Requests: ", requests);

				this.workload.push({
					state: STATE_INIT,
					opts: opts,
					name: name,
				});
			} else {
				this.workload.push({
					state: STATE_INIT,
					opts: opts,
					data: data.subarray(offset, offset + optlen),
				});
				offset += optlen;
			}
		}

		this.state = STATE_NORMAL;

		return offset;
	}

	handleNormalPacket() {
		const data = this.fullbuffer;
		let offset = 0;

		const magic = data.readUInt32BE(offset);
		offset += 4;
		if (magic !== NBD_REQUEST_MAGIC) {
			console.log("Unknown magic: ", magic.toString(16));
			console.log("DATA::::", data);
			// Out of sync, reset the buffer
			this.fullbuffer = Buffer.alloc(0);
			return 0;
			x;
		}

		const requestType = data.readUInt32BE(offset);
		offset += 4;

		const handle = Buffer.from(data.subarray(offset, offset + 8));
		offset += 8;

		const from = data.readBigUInt64BE(offset);
		offset += 8;

		const len = data.readUint32BE(offset);
		offset += 4;

		const cmd = requestType & NBD_CMD_MASK_COMMAND;
		const flags = requestType & ~NBD_CMD_MASK_COMMAND;

		switch (cmd) {
			case NBD_CMD_WRITE:
				if (data.length - offset < len) {
					// Wait for more data to arrive
					return 0;
				}

				this.workload.push({
					state: STATE_NORMAL,
					cmd,
					flags,
					handle,
					from,
					len,
					data: data.subarray(offset, offset + len),
				});

				offset += len;
				break;

			case NBD_CMD_READ:
				this.workload.push({
					state: STATE_NORMAL,
					cmd,
					flags,
					handle,
					from,
					len,
				});
				break;

			case NBD_CMD_FLUSH:
				this.workload.push({
					state: STATE_NORMAL,
					cmd: cmd,
					flags,
					handle: handle,
				});
				break;

			case NBD_CMD_TRIM:
				this.workload.push({
					state: STATE_NORMAL,
					cmd,
					flags,
					handle,
					from,
					len,
				});
				break;

			case NBD_CMD_WRITE_ZEROES:
				this.workload.push({
					state: STATE_NORMAL,
					cmd,
					flags,
					handle,
					from,
					len,
				});
				break;

			default:
				console.log("Unknown command: %o", cmd);
				this.dataReply(NBD_REP_ERRNO_EINVAL, handle);
		}

		return offset;
	}

	unload() {
		this.connected = false;
		if (this.timer) {
			clearInterval(this.timer);
			this.timer = null;
		}
		this.unloadFunction(this.socket);
	}
}

const clients = [];

const serv1 = net.createServer((socket) => {
	let fullbuffer = Buffer.alloc(0);
	let state = 0;
	console.log("load");

	const instance = new NBDServer(socket, () => {
		console.log("unload");
		if (clients.includes(instance)) {
			clients.splice(clients.indexOf(instance), 1);
		}
	});

	clients.push(instance);
});

serv1.listen(10809);

const serv = net.createServer((socket) => {
	let fullbuffer = Buffer.alloc(0);
	let state = 0;
	console.log("load");

	const instance = new NBDServer(socket, () => {
		console.log("unload");
		if (clients.includes(instance)) {
			clients.splice(clients.indexOf(instance), 1);
		}
	});

	clients.push(instance);
});

serv.listen("/tmp/nbdev.sock");

NBDExports.add("device0", {
	size: BigInt(1024 * 1024),
	name: "Tester litt",
});
NBDExports.add("device1", {
	size: BigInt(1024 * 1024 * 1024),
	name: "Tester litt",
});
