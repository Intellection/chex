# GitHub Release Preparation Plan for Natch

## Status: In Progress

### Completed ✅

#### Phase 1: Licensing & Attribution
- [x] Created `LICENSE` file with MIT license
- [x] Created `THIRD_PARTY_NOTICES.md` documenting all dependencies
- [x] Updated `mix.exs` with licenses field and package metadata

#### Phase 2: clickhouse-cpp Dependency Management
- [x] Added clickhouse-cpp as git submodule at `native/clickhouse-cpp`
- [x] Updated `CMakeLists.txt` with flexible path resolution:
  - Checks `CLICKHOUSE_CPP_DIR` environment variable first
  - Falls back to git submodule location
  - Provides clear error messages
- [x] Removed backward compatibility code for old hardcoded paths
- [x] Updated README with submodule initialization instructions

#### Phase 3: CI/CD - Testing with Valgrind
- [x] Created `.github/workflows/test.yml`:
  - Matrix testing on Elixir 1.17-1.18, OTP 26-27
  - ClickHouse service container
  - Separate valgrind job with Docker
  - Memory leak detection and artifact upload
- [x] Created `CHANGELOG.md` for v0.2.0
- [x] Updated `.gitignore` for valgrind artifacts

---

### Remaining Work 🚧

#### Phase 4: Prebuilt Binary System (NEXT)

**Step 1: Update mix.exs with precompiler configuration**
```elixir
def project do
  [
    # ... existing config ...
    version: @version,
    compilers: [:elixir_make] ++ Mix.compilers(),  # Note: Do NOT add :cc_precompiler here

    # Precompiler configuration
    make_precompiler: {:nif, CCPrecompiler},
    make_precompiler_url: "https://github.com/YOUR_ORG/natch/releases/download/v#{@version}/@{artefact_filename}",
    make_precompiler_nif_versions: [versions: ["2.16", "2.17"]],
    make_nif_filename: "natch_fine",  # Name of the .so/.dll file (without extension)
    make_precompiler_priv_paths: ["natch_fine.*"],  # Files to include from priv/ in tarball
    # ... rest of config ...
  ]
end

defp package do
  [
    # ... existing package config ...
    # Note: checksum-*.exs pattern will match the generated file
    files: ~w(lib priv native .formatter.exs mix.exs README.md LICENSE
              THIRD_PARTY_NOTICES.md CHANGELOG.md checksum-*.exs),  # ⚠️ checksum file is critical!
  ]
end

defp deps do
  [
    # ... existing deps ...
    {:cc_precompiler, "~> 0.1.0", runtime: false},
    # ...
  ]
end
```

**Key Configuration Details:**

- `make_precompiler: {:nif, CCPrecompiler}` - Enables precompilation system
- `make_precompiler_url` - Template URL where binaries are hosted (GitHub Releases)
  - `@version` is replaced with project version (e.g., "0.2.1")
  - `@{artefact_filename}` is replaced with platform-specific name at runtime
- `make_precompiler_nif_versions` - List of NIF versions to build for
- `make_nif_filename` - Base name of your NIF library (matches CMake output)
- `make_precompiler_priv_paths` - Glob patterns for files to include in tarball
- **CRITICAL:** Add `checksum-*.exs` to the `files:` list in package configuration
- **IMPORTANT:** Add `checksum-*.exs` to `.gitignore` (generated per release, not tracked in git)

**NIF Version Reference:**
- OTP 24 → NIF 2.15
- OTP 25 → NIF 2.16
- OTP 26-28 → NIF 2.17

**Step 2: Create precompile workflow**
File: `.github/workflows/precompile.yml`

**Strategy:** Build for many platforms via cross-compilation, test on platforms we have runners for.

**Tested Platforms (run tests in CI):**
- ✅ x86_64-linux-gnu (ubuntu-latest)
- ✅ aarch64-linux-gnu (ubuntu-24.04-arm)
- ✅ x86_64-apple-darwin (macos-13)
- ✅ aarch64-apple-darwin (macos-14)

**Cross-Compiled Platforms (no tests):**
- ⚠️ armv7l-linux-gnueabihf (32-bit ARM)
- ⚠️ riscv64-linux-gnu (RISC-V 64-bit)
- ⚠️ i686-linux-gnu (32-bit x86)

```yaml
name: Precompile NIFs

on:
  push:
    tags:
      - 'v*'

jobs:
  # Build and test on x86_64 Linux, cross-compile for other Linux architectures
  linux-x86:
    name: Linux x86_64 + cross-compile
    runs-on: ubuntu-latest
    strategy:
      matrix:
        otp: ["25.0", "26.2", "27.2"]

    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive

      - uses: erlef/setup-beam@v1
        with:
          otp-version: ${{ matrix.otp }}
          elixir-version: "1.18.4"

      - name: Install cross-compilation toolchains
        run: |
          sudo apt-get update
          sudo apt-get install -y \
            build-essential cmake libssl-dev \
            gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
            gcc-arm-linux-gnueabihf g++-arm-linux-gnueabihf \
            gcc-riscv64-linux-gnu g++-riscv64-linux-gnu \
            gcc-i686-linux-gnu g++-i686-linux-gnu

      - name: Install dependencies
        run: mix deps.get

      - name: Compile (for native x86_64 tests)
        run: mix compile

      - name: Run tests on x86_64
        run: mix test --exclude integration
        env:
          CLICKHOUSE_HOST: localhost
          CLICKHOUSE_PORT: 9000

      - name: Precompile NIFs (native + cross-compile)
        run: |
          export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
          mkdir -p $ELIXIR_MAKE_CACHE_DIR
          MIX_ENV=prod mix elixir_make.precompile

      - name: Upload artifacts to release
        uses: softprops/action-gh-release@v1
        if: startsWith(github.ref, 'refs/tags/')
        with:
          files: cache/*.tar.gz
          draft: true

    services:
      clickhouse:
        image: clickhouse/clickhouse-server:latest
        env:
          CLICKHOUSE_USER: default
          CLICKHOUSE_PASSWORD: ""
          CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
        ports:
          - 9000:9000
          - 8123:8123
        options: >-
          --health-cmd "clickhouse-client --query 'SELECT 1'"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

  # Build and test on ARM64 Linux
  linux-arm:
    name: Linux ARM64 (aarch64)
    runs-on: ubuntu-24.04-arm
    strategy:
      matrix:
        otp: ["25.0", "26.2", "27.2"]

    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive

      - uses: erlef/setup-beam@v1
        with:
          otp-version: ${{ matrix.otp }}
          elixir-version: "1.18.4"

      - name: Install dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y build-essential cmake libssl-dev
          mix deps.get

      - name: Compile
        run: mix compile

      - name: Run tests on ARM64
        run: mix test --exclude integration
        env:
          CLICKHOUSE_HOST: localhost
          CLICKHOUSE_PORT: 9000

      - name: Precompile NIFs
        run: |
          export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
          mkdir -p $ELIXIR_MAKE_CACHE_DIR
          MIX_ENV=prod mix elixir_make.precompile

      - name: Upload artifacts to release
        uses: softprops/action-gh-release@v1
        if: startsWith(github.ref, 'refs/tags/')
        with:
          files: cache/*.tar.gz
          draft: true

    services:
      clickhouse:
        image: clickhouse/clickhouse-server:latest
        env:
          CLICKHOUSE_USER: default
          CLICKHOUSE_PASSWORD: ""
          CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
        ports:
          - 9000:9000
          - 8123:8123
        options: >-
          --health-cmd "clickhouse-client --query 'SELECT 1'"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

  # Build and test on macOS Intel
  macos-intel:
    name: macOS Intel (x86_64)
    runs-on: macos-13
    strategy:
      matrix:
        otp: ["25.0", "26.2", "27.2"]

    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive

      - uses: erlef/setup-beam@v1
        with:
          otp-version: ${{ matrix.otp }}
          elixir-version: "1.18.4"

      - name: Install dependencies
        run: |
          brew install cmake openssl clickhouse
          mix deps.get

      - name: Start ClickHouse
        run: |
          brew services start clickhouse
          # Wait for ClickHouse to be ready
          sleep 10

      - name: Compile
        run: mix compile

      - name: Run tests on x86_64 macOS
        run: mix test --exclude integration

      - name: Precompile NIFs
        run: |
          export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
          mkdir -p $ELIXIR_MAKE_CACHE_DIR
          MIX_ENV=prod mix elixir_make.precompile

      - name: Upload artifacts to release
        uses: softprops/action-gh-release@v1
        if: startsWith(github.ref, 'refs/tags/')
        with:
          files: cache/*.tar.gz
          draft: true

  # Build and test on macOS Apple Silicon
  macos-arm:
    name: macOS Apple Silicon (ARM64)
    runs-on: macos-14
    strategy:
      matrix:
        otp: ["25.0", "26.2", "27.2"]

    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive

      - uses: erlef/setup-beam@v1
        with:
          otp-version: ${{ matrix.otp }}
          elixir-version: "1.18.4"

      - name: Install dependencies
        run: |
          brew install cmake openssl clickhouse
          mix deps.get

      - name: Start ClickHouse
        run: |
          brew services start clickhouse
          # Wait for ClickHouse to be ready
          sleep 10

      - name: Compile
        run: mix compile

      - name: Run tests on ARM64 macOS
        run: mix test --exclude integration

      - name: Precompile NIFs
        run: |
          export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
          mkdir -p $ELIXIR_MAKE_CACHE_DIR
          MIX_ENV=prod mix elixir_make.precompile

      - name: Upload artifacts to release
        uses: softprops/action-gh-release@v1
        if: startsWith(github.ref, 'refs/tags/')
        with:
          files: cache/*.tar.gz
          draft: true
```

**Critical Implementation Details:**

1. **ELIXIR_MAKE_CACHE_DIR environment variable:**
   - **MUST** be set before running `mix elixir_make.precompile`
   - Specifies where precompiled `.tar.gz` files are written
   - Default behavior won't work in CI without this
   - The directory MUST exist before running precompile

2. **GitHub Actions Action:**
   - Use `softprops/action-gh-release@v1` for uploading
   - `draft: true` creates a draft release (review before publishing)
   - `files: cache/*.tar.gz` uploads all generated artifacts

3. **Cross-compilation on Linux:**
   - Install toolchains: `gcc-aarch64-linux-gnu`, `gcc-arm-linux-gnueabihf`, etc.
   - cc_precompiler auto-detects installed toolchains by searching PATH
   - Each toolchain enables building for that target architecture
   - Native x86_64 build happens automatically

4. **Binary Naming Convention:**
   ```
   {project_name}-nif-{nif_version}-{arch}-{os}-{version}.tar.gz

   Examples (tested platforms):
   - natch-nif-2.16-x86_64-linux-gnu-0.2.1.tar.gz
   - natch-nif-2.17-aarch64-linux-gnu-0.2.1.tar.gz
   - natch-nif-2.17-x86_64-apple-darwin-0.2.1.tar.gz
   - natch-nif-2.17-aarch64-apple-darwin-0.2.1.tar.gz

   Examples (cross-compiled):
   - natch-nif-2.16-armv7l-linux-gnueabihf-0.2.1.tar.gz
   - natch-nif-2.17-riscv64-linux-gnu-0.2.1.tar.gz
   - natch-nif-2.16-i686-linux-gnu-0.2.1.tar.gz
   ```

   The naming is automatic - elixir_make generates based on detected platform.

   **Expected artifacts per release:** ~18-21 binaries total (depends on successful cross-compilation).

5. **Tarball Contents:**
   ```
   natch-nif-2.17-x86_64-linux-gnu-0.2.1.tar.gz
   └── natch_fine.so       # The compiled NIF library
       (other files matching make_precompiler_priv_paths patterns)
   ```

6. **Testing Locally Before CI:**
   ```bash
   # Test precompilation on your platform
   export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
   mkdir -p cache
   MIX_ENV=prod mix elixir_make.precompile

   # Check generated artifacts
   ls -lh cache/
   # Should see: natch-nif-2.17-aarch64-apple-darwin-0.2.1.tar.gz (or similar)

   # Inspect contents
   tar -tzf cache/natch-nif-*.tar.gz
   ```

**Step 3: Common Pitfalls and Troubleshooting**

**Critical Mistakes to Avoid:**

1. **Forgetting ELIXIR_MAKE_CACHE_DIR:**
   ```bash
   # ❌ WRONG - won't work in CI
   MIX_ENV=prod mix elixir_make.precompile

   # ✅ CORRECT
   export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
   mkdir -p $ELIXIR_MAKE_CACHE_DIR
   MIX_ENV=prod mix elixir_make.precompile
   ```

2. **Missing checksum.exs from Hex package:**
   - If `checksum.exs` is not in the `files:` list, users will always compile from source
   - The precompilation system silently falls back without error
   - **Always verify:** `mix hex.build` and check the tarball contents

3. **Wrong make_nif_filename:**
   - Must match the actual NIF filename without extension
   - Check what CMake outputs: `natch_fine.so` → use `"natch_fine"`
   - Mismatch causes precompiled binary to not be found

4. **Incorrect make_precompiler_url:**
   - Must use GitHub Releases URL format exactly
   - Must include `v` prefix for version tag: `v#{@version}`
   - Must use `@{artefact_filename}` placeholder (not `#{artefact_filename}`)

5. **Building before creating GitHub Release:**
   - The checksum task downloads from GitHub Releases
   - You MUST wait for GitHub Actions to finish and create the release
   - If you run `mix elixir_make.checksum` too early, it will fail

6. **Cross-compilation toolchain confusion:**
   - On Linux, native build happens automatically
   - Cross-builds only happen if toolchains are installed
   - Missing a toolchain means that platform won't be built (no error!)
   - Check build output carefully for which platforms were built

**Testing the Complete Flow Locally:**

```bash
# 1. Test precompilation on your platform
export ELIXIR_MAKE_CACHE_DIR=$(pwd)/test_cache
mkdir -p $ELIXIR_MAKE_CACHE_DIR
MIX_ENV=prod mix elixir_make.precompile

# 2. Verify artifact was created
ls -lh test_cache/
# Should see: natch-nif-2.17-aarch64-apple-darwin-0.2.1.tar.gz

# 3. Inspect tarball contents
tar -tzf test_cache/natch-nif-*.tar.gz
# Should see: natch_fine.so (or .dylib/.dll)

# 4. Test installation from tarball
rm -rf _build deps priv/*.so
mix deps.clean natch --unlock
mix deps.get
# Should extract from tarball instead of compiling

# 5. Verify it works
mix test
```

**Debugging Cross-Compilation:**

```bash
# Check which cross-compilers are available
which aarch64-linux-gnu-gcc
which arm-linux-gnueabihf-gcc

# Try building for specific target
export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ \
  MIX_ENV=prod mix elixir_make.precompile

# Check CMake output for which targets it's building
# Look for lines like: "-- Building for target: aarch64-linux-gnu"
```

---

#### Phase 5: Release Automation

**Step 1: Create release workflow**
File: `.github/workflows/release.yml`
- Creates draft release on tag
- Waits for precompile workflow
- Generates release notes from commits
- Manual approval step for Hex publish

**Step 2: Release checklist**
File: `RELEASING.md`
```markdown
1. Update CHANGELOG.md
2. Update version in mix.exs
3. Run: mix test --exclude integration
4. Commit: "Release v0.X.Y"
5. Tag: git tag -a v0.X.Y -m "Release v0.X.Y"
6. Push: git push origin main --tags
7. Monitor GitHub Actions
8. Approve and publish draft release
9. Run: mix hex.publish
```

---

#### Phase 6: Documentation

**Files to Update:**
1. `README.md` - Add troubleshooting section for build issues
2. `CONTRIBUTING.md` - Development workflow, submodule management
3. `docs/` - Architecture, NIF safety, performance benchmarks

---

## Complete Release Workflow: End-to-End

This section documents the complete flow from triggering a release to end-user installation.

### Phase 1: Triggering a Release (Maintainer)

```bash
# 1. Update version in mix.exs
# version: "0.2.1"

# 2. Update CHANGELOG.md with release notes

# 3. Commit and tag
git add mix.exs CHANGELOG.md
git commit -m "Release v0.2.1"
git tag -a v0.2.1 -m "Release v0.2.1"
git push origin main --tags
```

**Trigger:** Pushing a git tag matching `v*` pattern starts the automated build process.

### Phase 2: Building Artifacts (GitHub Actions - Automated)

**Workflow:** `.github/workflows/precompile.yml` (to be created)

**Build Matrix:** Four parallel jobs with testing + cross-compilation:
- **linux-x86** (ubuntu-latest): Tests x86_64, cross-compiles armv7l, riscv64, i686
- **linux-arm** (ubuntu-24.04-arm): Tests aarch64
- **macos-intel** (macos-13): Tests x86_64
- **macos-arm** (macos-14): Tests aarch64

**Steps in each job:**
```yaml
- Checkout with submodules (recursive)
- Setup Elixir/OTP for target version
- Install system dependencies (cmake, compilers, libssl-dev)
- Install Elixir dependencies: mix deps.get
- Set cache directory: export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
- Build precompiled binary: MIX_ENV=prod mix elixir_make.precompile
- Upload to GitHub Release: cache/*.tar.gz files
```

**Artifacts Generated:** 18-21 binaries per release with naming pattern:
```
natch-nif-{nif_version}-{arch}-{os}-{version}.tar.gz

Examples:
- natch-nif-2.16-x86_64-linux-gnu-0.2.1.tar.gz
- natch-nif-2.17-aarch64-apple-darwin-0.2.1.tar.gz
- natch-nif-2.17-x86_64-windows-msvc-0.2.1.tar.gz
```

**NIF Version Mapping:**
- NIF 2.16 = OTP 25
- NIF 2.17 = OTP 26-28

**Storage Location:** GitHub Releases at:
```
https://github.com/YOUR_ORG/natch/releases/download/v0.2.1/{artifact_name}.tar.gz
```

These artifacts are **publicly accessible** (no authentication required for downloads).

### Phase 3: Generating Checksum File (Maintainer)

**After GitHub Actions completes successfully:**

```bash
# Download all artifacts and generate checksums
# This creates checksum-natch.exs in your working directory
MIX_ENV=prod mix elixir_make.checksum --all --ignore-unavailable
```

**Note:** The checksum file is generated locally and NOT committed to git (add `checksum-*.exs` to `.gitignore`).

**What this does:**
1. Downloads all precompiled binaries from the GitHub Release
2. Calculates SHA256 hash for each artifact
3. Generates `checksum.exs` file with content like:

```elixir
%{
  "natch-nif-2.16-aarch64-apple-darwin-0.2.1.tar.gz" =>
    "sha256:a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
  "natch-nif-2.17-x86_64-linux-gnu-0.2.1.tar.gz" =>
    "sha256:f6e5d4c3b2a1098765fedcba4321098765fedcba4321098765fedcba43210987",
  # ... one entry per platform/NIF combination
}
```

**⚠️ CRITICAL:** The checksum file **MUST** be:
- In the `files:` list in `mix.exs` (e.g., `checksum-*.exs`)
- In `.gitignore` (not tracked in git)
- Present in your working directory when you run `mix hex.publish`

### Phase 4: Publishing to Hex.pm (Maintainer)

```bash
# Verify package contents
mix hex.build

# Publish to Hex.pm
mix hex.publish
```

**What gets uploaded to Hex.pm:**
- Elixir source code (lib/, test/)
- Native code source (native/natch_fine/)
- Build configuration (mix.exs, Makefile, CMakeLists.txt)
- **checksum.exs** (enables precompiled binary downloads)
- Documentation and metadata

**Critical mix.exs configuration:**
```elixir
def project do
  [
    # ...
    make_precompiler: {:nif, CCPrecompiler},
    make_precompiler_url: "https://github.com/YOUR_ORG/natch/releases/download/v#{@version}/@{artefact_filename}",
    make_precompiler_nif_versions: [versions: ["2.16", "2.17"]],
    # ...
  ]
end
```

The `@{artefact_filename}` placeholder gets dynamically replaced with the platform-specific artifact name during installation.

### Phase 5: End-User Installation (Automatic)

**User adds dependency:**
```elixir
# mix.exs
def deps do
  [
    {:natch, "~> 0.2.1"}
  ]
end
```

**User installs:**
```bash
mix deps.get
```

**What happens behind the scenes:**

1. **Download from Hex.pm:**
   - Mix downloads the package source code
   - Includes `checksum.exs` file

2. **Platform Detection:**
   - Detects OS: `darwin`, `linux`, `windows`
   - Detects architecture: `x86_64`, `aarch64`, etc.
   - Detects NIF version from current Erlang/OTP installation

3. **Construct Download URL:**
   - Takes `make_precompiler_url` template from mix.exs
   - Substitutes version: `v0.2.1`
   - Substitutes artifact name: `natch-nif-2.17-x86_64-linux-gnu-0.2.1.tar.gz`
   - Result: `https://github.com/YOUR_ORG/natch/releases/download/v0.2.1/natch-nif-2.17-x86_64-linux-gnu-0.2.1.tar.gz`

4. **Download Precompiled Binary:**
   - Fetches `.tar.gz` from GitHub Releases
   - Public download (no authentication)

5. **Verify Checksum:**
   - Calculates SHA256 of downloaded file
   - Looks up expected hash in `checksum.exs`
   - ✅ Match: Proceeds to extraction
   - ❌ Mismatch: Fails with integrity error

6. **Extract to priv/:**
   ```
   deps/natch/priv/natch_fine.so
   ```

7. **Done!** Installation complete in 2-5 seconds (vs 2-5 minutes for compilation)

**Fallback to Source Compilation:**

If no precompiled binary exists for the user's platform:
- elixir_make falls back to traditional compilation
- Executes `make` commands from Makefile
- Requires C++ build tools (cmake, compiler, etc.)
- Takes 2-5 minutes but ensures all platforms work

**Force Source Build:**
```bash
NATCH_BUILD=1 mix deps.get
```

### Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│ PHASE 1: Maintainer pushes git tag v0.2.1                       │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│ PHASE 2: GitHub Actions (.github/workflows/precompile.yml)      │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │Linux x86_64 │  │macOS ARM64  │  │Windows x64  │  ... (8-12) │
│  │NIF 2.16+2.17│  │NIF 2.16+2.17│  │NIF 2.16+2.17│             │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘             │
│         │                │                │                      │
│         └────────────────┴────────────────┘                      │
│                          │                                       │
│         Upload .tar.gz files to GitHub Releases                  │
│         https://github.com/ORG/natch/releases/tag/v0.2.1         │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│ PHASE 3: Maintainer generates checksums                          │
│                                                                  │
│  $ mix elixir_make.checksum --all --ignore-unavailable          │
│  - Downloads all artifacts from GitHub Release                   │
│  - Calculates SHA256 for each                                   │
│  - Creates checksum-natch.exs in working directory               │
│  (NOT committed to git - it's ephemeral per release)            │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│ PHASE 4: Maintainer publishes to Hex.pm                         │
│                                                                  │
│  $ mix hex.publish                                              │
│  Uploads: source code + checksum.exs + mix.exs config           │
│  Package available at: https://hex.pm/packages/natch             │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│ PHASE 5: End user installs                                      │
│                                                                  │
│  $ mix deps.get                                                 │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ 1. Download package from Hex.pm (source + checksum.exs)    │ │
│  │ 2. Detect platform: x86_64-linux-gnu, NIF 2.17            │ │
│  │ 3. Build URL from make_precompiler_url template            │ │
│  │ 4. Download .tar.gz from GitHub Releases                   │ │
│  │ 5. Verify SHA256 against checksum.exs                      │ │
│  │ 6. Extract to deps/natch/priv/natch_fine.so                 │ │
│  │ 7. Done! ✅ (2-5 seconds, no compilation)                  │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### Key Benefits

**For End Users:**
- ⚡ Fast installation (seconds vs minutes)
- 🛠️ No build tools required (cmake, C++ compiler, etc.)
- 🔒 Integrity verification via checksums
- 🔄 Automatic fallback to source compilation

**For Maintainers:**
- 🤖 Fully automated via GitHub Actions
- 🌍 Broad platform support (8-12 combinations)
- 📦 Simple release process (git tag → automation)
- 🔐 GitHub-hosted artifacts (free, reliable)

### Troubleshooting

**If precompiled binary download fails:**
- Falls back to source compilation automatically
- User needs: cmake, C++17 compiler, libssl-dev
- Can force with: `NATCH_BUILD=1 mix deps.get`

**If checksum verification fails:**
- Indicates corrupted download or tampered binary
- User should retry or report issue
- Never proceeds with mismatched checksum

**If no binary for platform:**
- Automatically compiles from source
- Expected for niche platforms (FreeBSD, Alpine, etc.)
- Can submit PR to add platform to build matrix

---

## Testing Checklist Before Release

- [ ] All 316 tests pass locally
- [ ] Tests pass on GitHub Actions (all matrix combinations)
- [ ] Valgrind reports 0 memory leaks
- [ ] Prebuilt binaries work on all platforms
- [ ] Source build works with `NATCH_BUILD=true`
- [ ] Documentation is accurate and complete
- [ ] CHANGELOG is up to date
- [ ] Mix hex.build succeeds

---

## Technical Notes

### License Compatibility Matrix
| Dependency | License | Compatible with MIT? |
|------------|---------|---------------------|
| clickhouse-cpp | Apache 2.0 | ✅ Yes |
| OpenSSL | Apache 2.0 | ✅ Yes |
| lz4 | BSD-2-Clause | ✅ Yes |
| zstd | BSD-3-Clause | ✅ Yes |
| cityhash | MIT | ✅ Yes |
| abseil-cpp | Apache 2.0 | ✅ Yes |

### clickhouse-cpp Version
- Current: v2.6.0 (commit 6919524)
- Submodule location: `native/clickhouse-cpp`
- Size: ~8.4MB

### Build Requirements by Platform
- **macOS:** Xcode Command Line Tools, CMake via Homebrew
- **Linux:** build-essential, cmake, libssl-dev
- **Windows:** Not currently supported (future work)

---

## Addressing Workflow Questions

### Q1: Is the checksum generation automated anywhere?

**Short answer:** Most projects use a semi-automated workflow with manual checksum generation.

**Why it's manual:**
- The checksum file should **NOT be in git** (add to `.gitignore`)
- But it **MUST be in the Hex package** (in `files:` list)
- This allows maintainers to verify artifact integrity before publishing
- Hex may show important warnings during publish that need human review

**Standard workflow:**
1. Push tag → CI builds binaries → uploads to GitHub Release
2. Locally: `MIX_ENV=prod mix elixir_make.checksum --all` (generates checksum.exs)
3. Locally: `mix hex.publish` (includes checksum.exs in the package)
4. Add `checksum-*.exs` to `.gitignore` (it's regenerated each release)

**Could you automate it?** Yes, with a workflow like:
```yaml
# After all build jobs complete
- name: Generate checksums
  run: MIX_ENV=prod mix elixir_make.checksum --all --ignore-unavailable

- name: Publish to Hex
  env:
    HEX_API_KEY: ${{ secrets.HEX_API_KEY }}
  run: mix hex.publish --yes
```

**Tradeoffs:**
- ✅ Fully automated, no manual steps
- ❌ No human verification of artifacts
- ❌ No chance to review Hex warnings
- ❌ Can't test locally before publishing

**Recommendation:** Keep it semi-automated for first few releases, automate later once confident.

### Q2: Should you test NIFs on all platforms?

**The Problem:**
Cross-compiled binaries (aarch64, armv7l, riscv64, etc.) are built but **not tested** before publishing. This is risky!

**Chosen Strategy for Natch:**

**Build and test on 4 major platforms with real hardware:**
- ✅ x86_64-linux-gnu (ubuntu-latest)
- ✅ aarch64-linux-gnu (ubuntu-24.04-arm) ← ARM Linux runner
- ✅ x86_64-apple-darwin (macos-13)
- ✅ aarch64-apple-darwin (macos-14)

**Cross-compile additional architectures (no tests):**
- ⚠️ armv7l-linux-gnueabihf (32-bit ARM - RPi, etc.)
- ⚠️ riscv64-linux-gnu (RISC-V 64-bit)
- ⚠️ i686-linux-gnu (32-bit x86 - legacy systems)

**Rationale:**
- Covers ~95% of real-world deployments with tested binaries
- Cross-compiled architectures provide convenience for niche platforms
- Document in README that exotic architectures may have issues
- Users can always fall back to source compilation

**Total artifacts per release:** ~18-21 binaries
- 4 tested platforms × 3 NIF versions (2.15, 2.16, 2.17) = 12 tested
- 3 cross-compiled platforms × 3 NIF versions = 9 untested

**README disclosure:**
```markdown
## Platform Support

Natch provides precompiled binaries for the following platforms:

**Tested in CI (recommended):**
- Linux x86_64 (tested on Ubuntu)
- Linux ARM64 (tested on Ubuntu ARM)
- macOS x86_64 (Intel Macs)
- macOS ARM64 (Apple Silicon)

**Cross-compiled (best effort):**
- Linux ARMv7 (32-bit, e.g., Raspberry Pi)
- Linux RISC-V 64-bit
- Linux i686 (32-bit x86)

Cross-compiled binaries are not tested before release. If you encounter
issues, please compile from source or report the problem.
```

### Q3: Tag timing issue - is the tag one behind?

**Great catch!** But actually, it's not a problem because:

**The checksum file is NOT in git:**
```bash
# Add to .gitignore
checksum-*.exs
```

**Workflow timeline:**
```
Commit A: version: "0.2.1" in mix.exs
   ↓
Tag v0.2.1 points to Commit A
   ↓
CI builds binaries → GitHub Release v0.2.1
   ↓
Locally: mix elixir_make.checksum --all  (NOT committed)
   ↓
Locally: mix hex.publish  (publishes from Commit A + checksum.exs)
```

**Key insight:** `mix hex.publish` packages files from:
- Your working directory (includes checksum.exs)
- NOT strictly from the git tag

So the Hex package includes:
- Source from git tag v0.2.1
- checksum.exs from your working directory (ephemeral, not in git)

**This is why:**
- `checksum-*.exs` should be in `.gitignore`
- `checksum-*.exs` should be in `files:` list in mix.exs
- You generate it locally before publishing

**Alternative: Fully automated**
If you automate the whole workflow in GitHub Actions:
1. CI generates checksum.exs in the workflow
2. CI publishes to Hex with checksum.exs
3. checksum.exs never touches git at all

This works because the GitHub Actions runner has the source + builds the checksum file, then publishes both together.

---

## Future Enhancements (Post v0.2.0)

1. **Windows Support**
   - MSVC build configuration
   - Windows runners in CI

2. **Additional Platforms**
   - FreeBSD
   - Alpine Linux (musl libc)

3. **Build Optimizations**
   - ccache for faster CI builds
   - Build caching across workflow runs

4. **Artifact Signing**
   - GPG signatures for binaries
   - Cosign for Docker images
