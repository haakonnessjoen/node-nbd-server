nbd server
==========

This project started because I wanted to do block device driver in user-space (in node.js).

Maybe not the most performant way of doing a block driver, but this project might be a nice starting point if I am doing something in c++ or something similar later on.

# Usage

Currently, as a proof of concept, the block storage is stored in redis. Start a redis server, and then start the server:

```bash
rm /tmp/nbdev.sock ; node index
```

Then connect a device either using TCP:

```bash
modprobe nbd
nbd-client -N device0 -b 4096 -p -L 127.0.0.1 /dev/nbd0
```

Or via unix socket:

```bash
sudo nbd-client -N device0 -b 4096 -p -L -u /tmp/nbdev.sock /dev/nbd0
```

To later remove the block device:

```bash
sudo nbd-client -d /dev/ndb0
```