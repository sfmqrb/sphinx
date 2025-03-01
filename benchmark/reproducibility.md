# Reproducibility Guide

This document details the requirements, build instructions, environment setup, and procedures for generating figures to reproduce the experimental results presented in the paper.

## Requirements

Ensure that your system has the following software dependencies installed. Use **these versions and above**:

| Dependency     | Version Installed (or later) |
|---------------|-----------------------------|
| **Seaborn**    | 0.12.2                      |
| **Matplotlib** | 3.8.0                       |
| **Pandas**     | 2.1.4                       |
| **Numpy**      | 1.26.4                      |
| **GCC**        | 11.4.0                      |
| **CMake**      | 3.22.1                      |
| **Git**        | 2.34.1                      |
| **Python**     | 3.11.7                      |

Additionally, verify that **Bit Manipulation Instructions (BMI)** are enabled on your system.

> **Note:** For optimal performance, a Linux machine with at least 64GB of RAM is recommended.

## Building

For detailed build instructions, please refer to the [README.md](../README.md).

## Environment Setup

Before running the benchmarks, configure your environment by setting the appropriate flags in `./config/config.h`:

- **Memory Benchmarks:** Define `IN_MEMORY_FILE`
- **Multi-threading Benchmarks:** Define `ENABLE_MT`
- **Skewed Workload Benchmarks:** Define `ENABLE_BP_FOR_READ`

> **Note:** The append-only log address should be updated to point to the correct mounted location. (For Optane benchmarks, ensure that the path includes the name `optane`.)

## Running Benchmarks and Generating Figures

1. **Execute Benchmarks:**

   Run the benchmark binaries (e.g., `./build/benchmark_*`) with the corresponding configuration. Upon execution, several data directories will be created:
   
| Directory       | Description                                                 | Figure Reference  |
|---------------|-------------------------------------------------|----------------|
| data-lf       | Load factor benchmark                         | Figure 12      |
| data-ssd      | Performance of Sphinx in SSD                 | Figure 8       |
| data-optane   | Performance of Sphinx in Optane              | Figure 8       |
| data-skew     | Performance of Sphinx under skew             | Figure 9 (Part A) |
| data-tail     | Measured tail latency                        | Figure 10       |
| data-memory   | Memory overhead (Figure 7) & performance (Figure 8) | Figures 7 & 8  |
| data-extra-bits | Reserve Bits                          | Figure 11      |
| data-mt       | Concurrency                   | Figure 9 (Part B) |


2. **Generate Figures:**

   After completing all benchmark runs, generate the paper figures by executing the following Python scripts:
   
   - **`main_exp.py`** – Generates Figures 7 and 8
   - **`combined.py`** – Generates Figure 9
   - **`tail.py`** – Generates Figure 10
   - **`extra_bits.py`** – Generates Figure 11
   - **`lf.py`** – Generates Figure 12

   Ensure that the data directories and configuration settings are correct before running these scripts.

