# Sphinx: A Succinct Perfect Hash Index for x86

<p align="center">
Sphinx is a state-of-the-art succinct perfect hash table engineered for high performance on modern x86 CPUs. Its innovative encoding leverages rank and select primitives alongside auxiliary metadata to enable near-instantaneous hash table slot decoding. Moreover, Sphinx is designed to be expandable and easily parallelizable.
</p>

## Quickstart

To get started, simply clone the repository and navigate into the project directory:

```bash
git clone [repo-url]
cd sphinx
```

## Reproducablity
Benchmark outputs will be generated in the `build/benchmark/` directory. For complete instructions on reproducing the results described in the paper, see [reproducibility.md](benchmark/reproducibility.md).

## Compiling and Testing

After cloning the repository along with its submodules, follow these steps to build the project using CMake:

1. Create a build directory and navigate into it:
   ```bash
   mkdir build && cd build
   ```
2. Build the project with optimizations with:
   ```bash
   cmake --build . --parallel 10
   ```

To run tests, make sure that `ENABLE_MT` is defined in your configuration. Then, execute:
```bash
ctest .
```

## License

This project is licensed under the BSD 2-Clause License. See the [LICENSE](LICENSE) file for full details.
