# CHANGELOG

<!-- version list -->

## v0.2.2 (2026-09-08)

### Bug Fixes

- "Tests" badge ([#71](https://github.com/faststream-community/zMQTT/pull/71),
  [`dc9b459`](https://github.com/faststream-community/zMQTT/commit/dc9b459244478a36ec9fa14b06ce4607abc297eb))

### Chores

- Update README badges ([#69](https://github.com/faststream-community/zMQTT/pull/69),
  [`2c1bf3c`](https://github.com/faststream-community/zMQTT/commit/2c1bf3c9e0b4b75949bcc8ece64eedbfd1fb082a))

### Continuous Integration

- **deps**: Bump the github-actions group with 2 updates
  ([#94](https://github.com/faststream-community/zMQTT/pull/94),
  [`9c4001a`](https://github.com/faststream-community/zMQTT/commit/9c4001acdd1571beac8ea97975151db258b8d302))

- **deps**: Bump the github-actions group with 2 updates
  ([#70](https://github.com/faststream-community/zMQTT/pull/70),
  [`3341b00`](https://github.com/faststream-community/zMQTT/commit/3341b0033ce7671a0630b3e9078ec2b03a0a9fd3))

### Features

- **client**: Expose MQTT connection information
  ([#92](https://github.com/faststream-community/zMQTT/pull/92),
  [`a6daf60`](https://github.com/faststream-community/zMQTT/commit/a6daf60a1c5dab8cfb7cb6886199dfd6c1fbf978))


## v0.2.1 (2026-08-30)

### Bug Fixes

- **publish**: Raise MQTTPublishError on negative PUBACK/PUBREC reason codes.
  ([#67](https://github.com/faststream-community/zMQTT/pull/67),
  [`3aa3498`](https://github.com/faststream-community/zMQTT/commit/3aa349877b8c78ef128b5a921f4a84479a82e37d))

### Chores

- **tests**: Add htmlcov badge ([#64](https://github.com/faststream-community/zMQTT/pull/64),
  [`636dc6a`](https://github.com/faststream-community/zMQTT/commit/636dc6afa0974a6f296ce6b1eedfa7de21733f1a))

### Documentation

- Placed docs link to readme header ([#66](https://github.com/faststream-community/zMQTT/pull/66),
  [`f937323`](https://github.com/faststream-community/zMQTT/commit/f937323687cf1872cdf3a88782e300ff4e4b4379))


## v0.2.0 (2026-08-25)

### Bug Fixes

- Accept persistent session deliveries and wait for matching subscription on idle. Fixes #59
  ([#62](https://github.com/faststream-community/zMQTT/pull/62),
  [`d59204c`](https://github.com/faststream-community/zMQTT/commit/d59204c82cc8ac78b26ba0112156720065aeec40))

- Artemis last will test ([#55](https://github.com/faststream-community/zMQTT/pull/55),
  [`a0b9103`](https://github.com/faststream-community/zMQTT/commit/a0b910364f2c929a4c279d6ba0d145d230fcb5d8))

- Propagate terminal error to subscription
  ([#60](https://github.com/faststream-community/zMQTT/pull/60),
  [`68410a0`](https://github.com/faststream-community/zMQTT/commit/68410a07d52ab6c05c4957c119ebe8922cadfba2))

### Chores

- Speed up tests with pytest-xdist ([#54](https://github.com/faststream-community/zMQTT/pull/54),
  [`b05d644`](https://github.com/faststream-community/zMQTT/commit/b05d644cc400a5e61ea89499b611ef7314fd8d8d))

### Continuous Integration

- **deps**: Bump astral-sh/setup-uv from 9.0.0 to 10.0.1 in the github-actions group
  ([#56](https://github.com/faststream-community/zMQTT/pull/56),
  [`ed7dc1c`](https://github.com/faststream-community/zMQTT/commit/ed7dc1caac66e753907c00476fd7dcc374235a5e))

### Features

- Add callback for reconnect failing ([#61](https://github.com/faststream-community/zMQTT/pull/61),
  [`5c1c346`](https://github.com/faststream-community/zMQTT/commit/5c1c34601838744546e7f098c590a01c00a23d2f))

### Testing

- **artemis**: Skip flaky message ordering test
  ([#58](https://github.com/faststream-community/zMQTT/pull/58),
  [`55f7d5b`](https://github.com/faststream-community/zMQTT/commit/55f7d5b44a84caea8b90e30099555554f5b7f2e9))


## v0.1.1 (2026-08-16)

### Chores

- **ci**: Add integration with the oss board
  ([#49](https://github.com/faststream-community/zMQTT/pull/49),
  [`80b8b07`](https://github.com/faststream-community/zMQTT/commit/80b8b07fea9f68ec75e0f619feadd14f4cff74f2))

- **ci**: Fix token ([#50](https://github.com/faststream-community/zMQTT/pull/50),
  [`5eb07c5`](https://github.com/faststream-community/zMQTT/commit/5eb07c521be504f8f73928c5f9c8ae80cd12de94))

### Documentation

- Actualize README and documentation ([#48](https://github.com/faststream-community/zMQTT/pull/48),
  [`27fbda9`](https://github.com/faststream-community/zMQTT/commit/27fbda9f7a766f8b76f39791d2ae48046bb0e05a))

- Add badges to readme ([#51](https://github.com/faststream-community/zMQTT/pull/51),
  [`3083320`](https://github.com/faststream-community/zMQTT/commit/3083320d19eefffc826c5e36b70963143ad263f9))

### Features

- **client**: Expose Last Will configuration
  ([#52](https://github.com/faststream-community/zMQTT/pull/52),
  [`b71fd5c`](https://github.com/faststream-community/zMQTT/commit/b71fd5c7031713714b22d9bbdd58204c6ebf8806))

### Refactoring

- More explicit routing logic for responses and regular subscriptions
  ([#46](https://github.com/faststream-community/zMQTT/pull/46),
  [`fe1bee6`](https://github.com/faststream-community/zMQTT/commit/fe1bee646c3df7b50b36c78c0d69d8aed9c22ac3))


## v0.1.0 (2026-08-07)

### Chores

- Release pipeline, dependabot, minor ci improvements
  ([#37](https://github.com/faststream-community/zMQTT/pull/37),
  [`89f163d`](https://github.com/faststream-community/zMQTT/commit/89f163d9d53bb1933e81a34f56ee3343ec71da95))

### Continuous Integration

- Fix prepare release step ([#42](https://github.com/faststream-community/zMQTT/pull/42),
  [`e5c946f`](https://github.com/faststream-community/zMQTT/commit/e5c946f95bba64b36b97b6ae09aecdfd9cad0561))

- Remove auto pull request on release ([#43](https://github.com/faststream-community/zMQTT/pull/43),
  [`eab5a38`](https://github.com/faststream-community/zMQTT/commit/eab5a387aa97a98111d5d71dea11b9ca0c5f4340))

- **deps**: Bump the github-actions group with 2 updates
  ([#38](https://github.com/faststream-community/zMQTT/pull/38),
  [`3ec531e`](https://github.com/faststream-community/zMQTT/commit/3ec531ebb95b269f0fd7c17b3747ad9a100e0d12))

### Documentation

- Correct ack, reconnect, and request semantics
  ([#41](https://github.com/faststream-community/zMQTT/pull/41),
  [`2370525`](https://github.com/faststream-community/zMQTT/commit/237052509c1fe8c70f2507e467d6dcbc627a092d))

### Features

- Add retain handling to public api ([#40](https://github.com/faststream-community/zMQTT/pull/40),
  [`7065d7d`](https://github.com/faststream-community/zMQTT/commit/7065d7d6d67c01445351643970ce04fe9accbb83))

- Implement correct request/response pattern using correlation data
  ([#39](https://github.com/faststream-community/zMQTT/pull/39),
  [`3ad58a4`](https://github.com/faststream-community/zMQTT/commit/3ad58a4fc167a77a38666f555d42b04731aad319))

- Subscription trie ([#29](https://github.com/faststream-community/zMQTT/pull/29),
  [`add3108`](https://github.com/faststream-community/zMQTT/commit/add310803d7cd732c80838c1e4cc43fd89483c35))


## v0.0.6 (2026-07-26)

### Bug Fixes

- Bound the protocol-level delivery queue
  ([#33](https://github.com/faststream-community/zMQTT/pull/33),
  [`3dcd418`](https://github.com/faststream-community/zMQTT/commit/3dcd41872899f72a4aeb7eee41aa65bedbc019cf))

- Declare typing_extensions dependency for Python < 3.11
  ([#30](https://github.com/faststream-community/zMQTT/pull/30),
  [`a6dad1d`](https://github.com/faststream-community/zMQTT/commit/a6dad1df13cae58ee394ccc1038d9e2c6eb417c0))

- Raise on SUBACK failure codes instead of ignoring them
  ([#32](https://github.com/faststream-community/zMQTT/pull/32),
  [`2b4dd59`](https://github.com/faststream-community/zMQTT/commit/2b4dd597ef012e13fd4358e96bf187bcca871a29))

- Strip broker shared/decorator prefixes via a configurable allowlist
  ([#31](https://github.com/faststream-community/zMQTT/pull/31),
  [`194b530`](https://github.com/faststream-community/zMQTT/commit/194b530c9b76fa62fbdd11e82ed02cc99aed9221))

- Treat broker-initiated DISCONNECT as a disconnection, and fail fast
  ([`bb3ca58`](https://github.com/faststream-community/zMQTT/commit/bb3ca58cb868a1cdbf27452e1b58728baf025220))

### Documentation

- Fix link ([#28](https://github.com/faststream-community/zMQTT/pull/28),
  [`a2e42bb`](https://github.com/faststream-community/zMQTT/commit/a2e42bb9f71b2885ec6099f51895002455ebd87c))

### Features

- Mqtt 5 subscription identifiers
  ([`a3ebdda`](https://github.com/faststream-community/zMQTT/commit/a3ebdda90e7cae95873de5ced3da97cd798095c0))

### Testing

- Add EMQX to the broker test matrix ([#36](https://github.com/faststream-community/zMQTT/pull/36),
  [`950dbe2`](https://github.com/faststream-community/zMQTT/commit/950dbe2488e6e2479d7293106e5434677f609aa7))


## v0.0.5 (2026-06-08)

### Documentation

- Fix github url ([#25](https://github.com/faststream-community/zMQTT/pull/25),
  [`f8e174e`](https://github.com/faststream-community/zMQTT/commit/f8e174e2d6d95b5bb6ec62169a243d3d737fdb6a))

### Features

- Bound the CONNACK wait with a configurable connect_timeout
  ([#27](https://github.com/faststream-community/zMQTT/pull/27),
  [`3185373`](https://github.com/faststream-community/zMQTT/commit/3185373a7ace45e8b7648d013d9466791f4eb05e))

Co-authored-by: Claude Opus 4.8 (1M context) <noreply@anthropic.com>


## v0.0.4 (2026-04-10)

### Bug Fixes

- Accepting tls=None in client ([#22](https://github.com/faststream-community/zMQTT/pull/22),
  [`4e1fcc9`](https://github.com/faststream-community/zMQTT/commit/4e1fcc913f5a5a8d00d1f7a64e06e9a78f705ea6))


## v0.0.3 (2026-04-06)

### Bug Fixes

- 0x09 byte (correlation data) incompatibiity with the specification
  ([#20](https://github.com/faststream-community/zMQTT/pull/20),
  [`ebad1ba`](https://github.com/faststream-community/zMQTT/commit/ebad1ba63f14648e190a0d2ad645df21773cc1a7))


## v0.0.2 (2026-04-05)

### Features

- Implement request-response pattern for mqtt 5
  ([#19](https://github.com/faststream-community/zMQTT/pull/19),
  [`1ce6826`](https://github.com/faststream-community/zMQTT/commit/1ce682699c58e352e173c67e048b93a34d7749ec))


## v0.0.1 (2026-04-03)

### Documentation

- Add link to readme, minor fixes ([#16](https://github.com/faststream-community/zMQTT/pull/16),
  [`e054875`](https://github.com/faststream-community/zMQTT/commit/e05487597f971dc9c8f9130158662c516947decc))

### Features

- Add support for shared subscriptions
  ([#18](https://github.com/faststream-community/zMQTT/pull/18),
  [`78199b6`](https://github.com/faststream-community/zMQTT/commit/78199b6c8d651bc173a50f53bea31e6a707891ba))

- Improve reconnection strategy ([#17](https://github.com/faststream-community/zMQTT/pull/17),
  [`2d97821`](https://github.com/faststream-community/zMQTT/commit/2d97821afb08aee425c52afebe1f8702745706f0))


## v0.0.1-alpha.5 (2026-03-24)

### Chores

- Refactor, remove useless tests, improve docs
  ([#14](https://github.com/faststream-community/zMQTT/pull/14),
  [`0a1862e`](https://github.com/faststream-community/zMQTT/commit/0a1862e7f3c116989494a2fedc52f2cd01ea58b6))

### Features

- Manual connection and subscription ([#15](https://github.com/faststream-community/zMQTT/pull/15),
  [`87c2bb8`](https://github.com/faststream-community/zMQTT/commit/87c2bb89f9c7f7a44d71ca1f744d589f1aed9740))


## v0.0.1-alpha.4 (2026-03-23)

### Chores

- Extend linting ([#13](https://github.com/faststream-community/zMQTT/pull/13),
  [`df3dde7`](https://github.com/faststream-community/zMQTT/commit/df3dde720c05b74c556670cd84e182273a61b071))

### Features

- Add windows and macos tests ([#12](https://github.com/faststream-community/zMQTT/pull/12),
  [`9854447`](https://github.com/faststream-community/zMQTT/commit/9854447a5102e1010524e16640fa76d35d1518f9))

- Queue limit ([#6](https://github.com/faststream-community/zMQTT/pull/6),
  [`b30be27`](https://github.com/faststream-community/zMQTT/commit/b30be2777b8d199168606fca5b3300f54bdd600f))


## v0.0.1-alpha.3 (2026-03-22)

### Chores

- Add 3.10 python compat
  ([`6a08554`](https://github.com/faststream-community/zMQTT/commit/6a08554fdef91e3a8a1edb3d43cf0aa1c6d1fb51))

- Add python 3.10 support ([#10](https://github.com/faststream-community/zMQTT/pull/10),
  [`5bfb461`](https://github.com/faststream-community/zMQTT/commit/5bfb461b67bdd4791a3744003062e3f924d7390a))

### Documentation

- Clarify subscribe routing and fix MQTT5 auth wording
  ([`343280a`](https://github.com/faststream-community/zMQTT/commit/343280a40a75c588b002e9c4931df1d59b2684bb))

- Clarify subscribe routing and fix MQTT5 auth wording
  ([#9](https://github.com/faststream-community/zMQTT/pull/9),
  [`34adbef`](https://github.com/faststream-community/zMQTT/commit/34adbef398dd35bbe3133b111435d42949e943fd))


## v0.0.1-alpha.2 (2026-03-21)

### Features

- Rename to zmqtt
  ([`4098df6`](https://github.com/faststream-community/zMQTT/commit/4098df6a175f00f1c5deb3b903c10c9ec7373f8b))

- Rename to zmqtt ([#8](https://github.com/faststream-community/zMQTT/pull/8),
  [`301b037`](https://github.com/faststream-community/zMQTT/commit/301b0378710de925c284767fdb8e53fa91c9cbc5))


## v0.0.1-alpha.1 (2026-03-21)

### Chores

- Add release pipeline
  ([`071b85d`](https://github.com/faststream-community/zMQTT/commit/071b85d167c6b26a8dd0c85bc08f4ea65f86958c))

chore: Add release pipeline

- Add release pipeline
  ([`77ae34f`](https://github.com/faststream-community/zMQTT/commit/77ae34f6da88f108d8547b61892a1af064cd09bb))

- Tests infra
  ([`8f3b14c`](https://github.com/faststream-community/zMQTT/commit/8f3b14cdc9eb2bd4d8c4f74404bc26232d6dc525))
