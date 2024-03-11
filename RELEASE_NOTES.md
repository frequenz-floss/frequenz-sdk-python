# Frequenz Python SDK Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The SDK is now using the microgrid API client from [`frequenz-client-microgrid`](https://github.com/frequenz-floss/frequenz-client-microgrid-python/). You should update your code if you are using the microgrid API client directly.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- A bug was fixed where the grid fuse was not created properly and would end up with a `max_current` with type `float` instead of `Current`.
