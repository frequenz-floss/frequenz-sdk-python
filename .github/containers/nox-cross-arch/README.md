# Cross-Arch Testing Containers

This directory containers dockerfiles that can be used in the CI to test the
python package in non-native machine architectures, e.g., `aarch64`.

The dockerfiles here follow a naming scheme so that they can be easily used in
build matrices in the CI, in `nox-cross-arch` job. The naming scheme is:

```
<arch>-<os>-python-<python-version>.Dockerfile
```

E.g.,

```
arm64-ubuntu-20.04-python-3.11.Dockerfile
```

If a dockerfile for your desired target architecture, OS, and python version
does not exist here, please add one before proceeding to add your options to the
test matrix.
