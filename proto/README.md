# proto/

`hub.proto` is a verbatim copy of `kraken/protocol/hub.proto` from the umbrella repository.

> **Builder call:** until the GitLab group exists this directory is a plain copy. Once the
> repositories are pushed it becomes a git submodule pointing at the umbrella repo
> (`git submodule add ../kraken.git proto-src` and a symlink or build.rs path change), so the hub,
> `kraken-messaging` and `kraken-messaging-rs` compile the same file. Do not edit it here; edit it
> in the umbrella repo and re-sync.
