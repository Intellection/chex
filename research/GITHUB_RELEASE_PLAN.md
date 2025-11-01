# GitHub Release Preparation Plan for Chex

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

**Step 1: Add cc_precompiler dependency**
```elixir
# In mix.exs deps
{:cc_precompiler, "~> 0.1.0", runtime: false}
```

**Step 2: Create precompile configuration module**
File: `lib/chex/nif.ex`
```elixir
defmodule Chex.NIF do
  @moduledoc false

  # NIF version mapping
  # OTP 24 -> NIF 2.15
  # OTP 25 -> NIF 2.16
  # OTP 26-27 -> NIF 2.17

  @available_targets [
    "aarch64-apple-darwin-nif-2.16",
    "aarch64-apple-darwin-nif-2.17",
    "x86_64-apple-darwin-nif-2.16",
    "x86_64-apple-darwin-nif-2.17",
    "aarch64-linux-gnu-nif-2.16",
    "aarch64-linux-gnu-nif-2.17",
    "x86_64-linux-gnu-nif-2.16",
    "x86_64-linux-gnu-nif-2.17"
  ]

  use CCPrecompiler,
    compilers: [:elixir_make],
    available_targets: @available_targets,
    cleanup: "make clean",
    force_build: System.get_env("CHEX_BUILD") in ["1", "true"]

  def current_target do
    # Detect platform, arch, NIF version
    # Return string like "x86_64-apple-darwin-nif-2.17"
  end
end
```

**Step 3: Update mix.exs**
```elixir
def project do
  [
    # ...
    compilers: [:cc_precompiler, :elixir_make] ++ Mix.compilers(),
    # ...
  ]
end
```

**Step 4: Create precompile workflow**
File: `.github/workflows/precompile.yml`
- Triggers on tag push (`v*`)
- Matrix build for all platforms
- Creates draft release with artifacts

**Implementation Details:**

1. **Platform Matrix:**
   - macOS x86_64: `macos-13` with OTP 26, 27
   - macOS ARM64: `macos-14` with OTP 26, 27
   - Linux x86_64: `ubuntu-20.04` with OTP 26, 27
   - Linux ARM64: `ubuntu-24.04-arm` with OTP 26, 27

2. **Build Steps per Platform:**
   ```yaml
   - Checkout with submodules
   - Setup Elixir/OTP
   - Install system dependencies
   - Build NIF: mix compile
   - Package: tar -czf chex-nif-$TARGET.tar.gz -C priv .
   - Generate checksums
   - Upload to GitHub release
   ```

3. **Binary Naming Convention:**
   ```
   chex-nif-{arch}-{os}-nif-{version}.tar.gz
   Examples:
   - chex-nif-x86_64-apple-darwin-nif-2.17.tar.gz
   - chex-nif-aarch64-linux-gnu-nif-2.17.tar.gz
   ```

4. **Checksums:**
   - Generate SHA256 for each artifact
   - Store in `checksums.txt` file
   - Required for Hex publication

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

## Testing Checklist Before Release

- [ ] All 316 tests pass locally
- [ ] Tests pass on GitHub Actions (all matrix combinations)
- [ ] Valgrind reports 0 memory leaks
- [ ] Prebuilt binaries work on all platforms
- [ ] Source build works with `CHEX_BUILD=true`
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
