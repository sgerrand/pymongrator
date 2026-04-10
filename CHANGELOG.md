# Changelog

## [0.3.0](https://github.com/sgerrand/pymongrator/compare/v0.2.1...v0.3.0) (2026-04-10)


### Features

* Add --dry-run flag to up and down commands ([#12](https://github.com/sgerrand/pymongrator/issues/12)) ([a144142](https://github.com/sgerrand/pymongrator/commit/a144142caa29682e3d8f40d7937cfa4c9cacdb2f))
* Add advisory locking to prevent concurrent migrations ([#16](https://github.com/sgerrand/pymongrator/issues/16)) ([e4d7d2b](https://github.com/sgerrand/pymongrator/commit/e4d7d2bec0a042965e0cd13dcb9352c092ac8978))
* Add auto-revert support to drop_index ([#17](https://github.com/sgerrand/pymongrator/issues/17)) ([8b8f3c7](https://github.com/sgerrand/pymongrator/commit/8b8f3c70a42346c64b88cd64df4fd8625637a6d2))
* Add drop_field, create_collection, and drop_collection ops ([#15](https://github.com/sgerrand/pymongrator/issues/15)) ([b1ec730](https://github.com/sgerrand/pymongrator/commit/b1ec73012292efd141b84049987392548b3e6aa6))


### Bug Fixes

* Close MongoClient connections in CLI commands ([#10](https://github.com/sgerrand/pymongrator/issues/10)) ([d5a20d1](https://github.com/sgerrand/pymongrator/commit/d5a20d1f877e984c681cfc5c90d12fd0f29fe05b))

## [0.2.1](https://github.com/sgerrand/pymongrator/compare/v0.2.0...v0.2.1) (2026-04-09)


### Bug Fixes

* Correct project URLs to match repository name ([#6](https://github.com/sgerrand/pymongrator/issues/6)) ([3f4d006](https://github.com/sgerrand/pymongrator/commit/3f4d006a2b420809afe700a224b6eb2307e10a73))
* **docs:** Create HTML output directory before copying site files ([#9](https://github.com/sgerrand/pymongrator/issues/9)) ([2340c12](https://github.com/sgerrand/pymongrator/commit/2340c12f8ad89c229141802a954f3fb053e49206))


### Documentation

* Add documentation site with Zensical and Read the Docs ([#8](https://github.com/sgerrand/pymongrator/issues/8)) ([e14077f](https://github.com/sgerrand/pymongrator/commit/e14077f0fd81c496492f92aff87362625b492d3b))

## [0.2.0](https://github.com/sgerrand/pymongrator/compare/v0.1.0...v0.2.0) (2026-04-08)

### Bug Fixes

* Read config from [mongrator] table written by init ([e7476be](https://github.com/sgerrand/pymongrator/commit/e7476be34bdf3e55adf11f496c7d21f09db6279c))
* Upgrade actions/download-artifact to v8.0.1 ([e3156df](https://github.com/sgerrand/pymongrator/commit/e3156dfa08d26aa360843d2957cb065f290d0f7c))

## [0.1.0](https://github.com/sgerrand/pymongrator/compare/a5376e63d6dff2fb5f689ea149d7b1c8c4f0775b...v0.1.0) (2026-04-08)

Initial release.

### Features

* initial MongoDB schema migration system ([a5376e6](https://github.com/sgerrand/pymongrator/commit/a5376e63d6dff2fb5f689ea149d7b1c8c4f0775b))

### Bug Fixes

* Install dependencies before running ty check ([1bc837b](https://github.com/sgerrand/pymongrator/commit/1bc837beb6c98b46502636a824ca03e8d20d818a))
* Update get_applied assertion to expect direction filter ([2e351b9](https://github.com/sgerrand/pymongrator/commit/2e351b9d489084b3ed1a0f8b752d7ac9a0b7f398))

### Documentation

* Add badge for continuous integration status ([bd1d93d](https://github.com/sgerrand/pymongrator/commit/bd1d93d2531a978d80dce098ec7827de6023185f))
