# amqp-base

This is a low-level Node.js / io.js library for maintaining a connection to an AMQP broker and a channel open.
It also includes components that use these connection/channel resumption mechanisms to provide reliable message listeners, capable of surviving multiple connection losses and channel errors.

For now, please refer to the usage examples in the `examples/` directory, until a proper tutorial is written.

# API reference

The API is documented using JSDoc3. In order to generate the corresponding HTML files, run the command below (you need the `jsdoc` npm package installed globally):

```
./gendoc.sh
```

Generated documents can then be found in the `jsdoc/` directory.

# License
MIT
