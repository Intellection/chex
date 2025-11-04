# Natch Release Process

This document describes the complete process for releasing a new version of Natch to Hex.pm with precompiled binaries.

## Prerequisites

- All tests passing locally (`mix test`)
- All tests passing on GitHub Actions
- CHANGELOG.md updated with new version
- No compiler warnings

## Release Steps

### 1. Update Version

Update the version in `mix.exs`:

```elixir
@version "0.X.Y"
```

### 2. Update CHANGELOG.md

Add a new section for the release:

```markdown
## [0.X.Y] - YYYY-MM-DD

### Added
- New features...

### Changed
- Changes to existing functionality...

### Fixed
- Bug fixes...
```

### 3. Commit and Tag

```bash
# Commit version bump
git add mix.exs CHANGELOG.md
git commit -m "Release v0.X.Y"

# Create annotated tag
git tag -a v0.X.Y -m "Release v0.X.Y"

# Push to GitHub (triggers precompile workflow)
git push origin main --tags
```

### 4. Wait for GitHub Actions

The `.github/workflows/precompile.yml` workflow will automatically:
- Build precompiled binaries for 7 platforms
- Upload them to the GitHub Release
- This takes approximately 10-15 minutes

Monitor the workflow at:
```
https://github.com/Intellection/natch/actions
```

Expected binaries:
- `natch-nif-2.17-x86_64-linux-gnu-0.X.Y.tar.gz`
- `natch-nif-2.17-aarch64-linux-gnu-0.X.Y.tar.gz`
- `natch-nif-2.17-x86_64-apple-darwin-0.X.Y.tar.gz`
- `natch-nif-2.17-aarch64-apple-darwin-0.X.Y.tar.gz`
- `natch-nif-2.17-armv7l-linux-gnueabihf-0.X.Y.tar.gz` (cross-compiled)
- `natch-nif-2.17-i686-linux-gnu-0.X.Y.tar.gz` (cross-compiled)
- `natch-nif-2.17-riscv64-linux-gnu-0.X.Y.tar.gz` (cross-compiled)

### 5. Generate Checksums

Once GitHub Actions completes:

```bash
# Generate checksums for all precompiled binaries
MIX_ENV=prod mix elixir_make.checksum --all --ignore-unavailable
```

This creates `checksum.exs` in your working directory with SHA256 hashes of all binaries.

**Important:**
- This file is in `.gitignore` (don't commit it)
- This file is in `mix.exs` package `files:` list (will be included in Hex package)

### 6. Verify Package

```bash
# Build the package
mix hex.build

# Verify checksum.exs is included
tar -tzf natch-0.X.Y.tar | grep checksum

# Should see: checksum.exs
```

### 7. Publish to Hex.pm

```bash
# Publish (will include checksum.exs)
mix hex.publish
```

Follow the prompts and confirm the publication.

### 8. Verify Installation

Create a test project to verify:

```bash
mkdir /tmp/natch-verify && cd /tmp/natch-verify
mix new . --app test_natch
```

Add to `mix.exs`:
```elixir
{:natch, "~> 0.X.Y"}
```

Test installation:
```bash
mix deps.get

# Look for: "Downloading precompiled NIF to ..."
# Should NOT see CMake/compiler output
```

Test functionality:
```bash
# With ClickHouse running on localhost:9000
iex -S mix

iex> {:ok, conn} = Natch.Connection.start_link(host: "localhost", port: 9000)
iex> Natch.Connection.execute(conn, "SELECT 1")
```

## Troubleshooting

### Checksums don't match
- Ensure GitHub Actions completed successfully
- Check the v0.X.Y release has all 7 binaries
- Try deleting local cache: `rm -rf ~/.cache/elixir_make/`
- Re-run: `MIX_ENV=prod mix elixir_make.checksum --all`

### Precompiled binary not downloading
- Verify checksum.exs is in the Hex package: `mix hex.build` and inspect
- Check GitHub release has public access to binaries
- Verify the URL template in mix.exs matches: `https://github.com/Intellection/natch/releases/download/v#{@version}/@{artefact_filename}`

### Build fails for a platform
- Cross-compiled platforms (armv7l, i686, riscv64) are best-effort
- Users on those platforms will fall back to source compilation
- This is expected and acceptable

## Post-Release

1. Announce on:
   - Elixir Forum
   - GitHub Discussions
   - Social media (optional)

2. Monitor for issues:
   - https://github.com/Intellection/natch/issues
   - Hex.pm package page

3. Clean up local artifacts:
   ```bash
   rm checksum.exs
   ```

## Quick Reference

```bash
# Full release in one go (after version updates committed)
git tag -a v0.X.Y -m "Release v0.X.Y"
git push origin main --tags

# Wait ~10-15 min for GitHub Actions...

MIX_ENV=prod mix elixir_make.checksum --all --ignore-unavailable
mix hex.build  # verify
mix hex.publish

# Clean up
rm checksum.exs
```

## Version Strategy

- **Patch (0.X.Y)**: Bug fixes, documentation, minor improvements
- **Minor (0.X.0)**: New features, new types, API additions (backward compatible)
- **Major (X.0.0)**: Breaking changes, API redesign

Current: v0.2.0 - First public release with precompiled binaries
