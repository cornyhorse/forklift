# Forklift Version Release Process

This guide describes how to release a new version of Forklift: preparing the version, creating the
GitHub release, and what the automated PyPI publish workflow checks before anything is uploaded.

## Prerequisites

Before starting a release, make sure you have:

- [ ] Write access to the GitHub repository
- [ ] All changes merged and tested on the release branch (CI green on Python 3.12, 3.13 and 3.14)
- [ ] Release notes prepared (the `## [Unreleased]` section of `CHANGELOG.md`)
- [ ] For a manual upload only: PyPI credentials (see [Manual upload](#52-manual-upload-fallback))

Most steps below are shown for a release called `v0.1.4`; substitute your version.

## Release Workflow

### 1. Pre-Release Preparation

#### 1.1 Branch management

```bash
# Work on your release branch (e.g. v0.1.4) and make sure it is up to date
git checkout v0.1.4
git pull origin v0.1.4

# Verify the current version
grep '^version' pyproject.toml
```

#### 1.2 Version number strategy

Follow semantic versioning (SemVer):

- **Patch (0.1.3 -> 0.1.4)**: bug fixes, minor improvements
- **Minor (0.1.4 -> 0.2.0)**: new features, backward compatible
- **Major (0.2.0 -> 1.0.0)**: breaking changes

### 2. Update Version Information

#### 2.1 Version

Edit the version in **both** places so they never disagree:

- `pyproject.toml`: `version = "0.1.4"`
- `src/forklift/__init__.py`: `__version__ = "0.1.4"`

The publish workflow fails if the release tag does not equal the version in `pyproject.toml`
(see [What the publish workflow checks](#what-the-publish-workflow-checks)).

#### 2.2 Changelog

Move the entries of `## [Unreleased]` in `CHANGELOG.md` into a new `## [0.1.4] - YYYY-MM-DD`
section ([Keep a Changelog](https://keepachangelog.com/) format: Added, Changed, Deprecated,
Removed, Fixed, Security) and leave a fresh, empty `## [Unreleased]` section above it.

#### 2.3 Documentation

Update any version-specific references in `README.md` and the docs, and check that the install
instructions (including the optional extras such as `forklift-etl[excel,sql,pandas,polars]`) are
still accurate.

### 3. Pre-Release Testing

```bash
# Install in development mode with every optional format and the test tooling
pip install -e ".[all,dev]"        # or: pip install -r requirements-dev.txt

# Run the test suite
python -m pytest tests/unit-tests

# Check formatting and lint like CI does
black --check --line-length 99 src/ tests/
isort --check-only --profile black --line-length 99 src/ tests/
flake8 src/ --max-line-length=99 --extend-ignore=E203,W503

# Build the distributions and verify them
rm -rf dist/ build/ *.egg-info/
pip install --upgrade build twine
python -m build
twine check dist/*
tar -tzf dist/forklift_etl-0.1.4.tar.gz      # source files, schema-standards/, CHANGELOG.md ...
```

Also exercise the key functionality with real data and, if you can, the oldest and newest
supported Python versions. CI runs the full suite on Python 3.12, 3.13 and 3.14.

### 4. GitHub Release

#### 4.1 Merge via pull request

Never tag directly from a feature branch: open a pull request, let CI pass, and merge to `main`.

```bash
git checkout main
git pull origin main
```

#### 4.2 Create and push the tag

```bash
# Annotated tag on the merge commit; the tag name is the version prefixed with "v"
git tag -a v0.1.4 -m "Release version 0.1.4"

# Use the explicit ref to avoid ambiguity with a branch of the same name
git push origin refs/tags/v0.1.4
```

#### 4.3 Create the GitHub release

1. Go to the repository on GitHub -> Releases -> "Draft a new release"
2. Choose the tag `v0.1.4` (it already exists)
3. Title: `Forklift v0.1.4`
4. Paste the release notes from `CHANGELOG.md`
5. Click "Publish release"

**Publishing the release is what triggers the PyPI publish workflow.**

### 5. PyPI Release

#### 5.1 Automated publish (preferred)

`.github/workflows/publish.yaml` runs when a GitHub release is published and uploads to PyPI with
[trusted publishing](https://docs.pypi.org/trusted-publishers/) (no API token is stored in the
repository). It stops before uploading anything if one of the checks below fails.

##### What the publish workflow checks

1. **Tag equals version** - the release tag (a leading `v` is ignored) must equal
   `project.version` in `pyproject.toml`, otherwise the workflow fails. Fix the version (or
   re-tag) and publish the release again.
2. **Build and metadata** - `python -m build` builds the sdist and wheel and `twine check` verifies
   their metadata.
3. **Install and test** - on Python 3.12, 3.13 and 3.14 the built wheel is installed with its extras,
   imported from outside the source tree, and `pytest tests/unit-tests` must pass.
4. **Publish** - only if everything above passed; the job that uploads is the only one with the
   `id-token: write` permission.

#### 5.2 Manual upload (fallback)

Use API tokens (never passwords), either in `~/.pypirc` or in environment variables:

```ini
# ~/.pypirc
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-your-api-token-here

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-your-test-api-token-here
```

```bash
# Alternative to .pypirc
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=pypi-your-api-token-here
```

```bash
rm -rf dist/ build/ *.egg-info/
python -m build
twine check dist/*

# Optional but recommended: try TestPyPI first
twine upload --repository testpypi dist/*
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ forklift-etl==0.1.4

# Production PyPI
twine upload dist/*
```

### 6. Post-Release

#### 6.1 Clean up branches

```bash
# Delete the local release branch
git branch -d v0.1.4

# Delete the remote release branch (use the explicit ref if a tag has the same name)
git push origin --delete refs/heads/v0.1.4
```

#### 6.2 Verify the release

```bash
pip install forklift-etl==0.1.4
python -c "import forklift; print(forklift.__version__)"
```

## Common Issues and Solutions

### `src refspec matches more than one`

You have both a branch and a tag with the same name. Push the tag with its full reference:

```bash
git push origin refs/tags/v0.1.4
git push origin --delete refs/heads/v0.1.4    # if the branch must go
```

### Publish workflow: "Release tag ... does not match the pyproject.toml version"

The tag and `project.version` disagree. Either the version was not bumped before tagging or the
tag has a typo. Delete the release and tag (or move the tag to the fixed commit), correct the
version, and publish the release again.

### SSH instead of HTTPS authentication

```bash
git remote -v                                                     # check the current remote
git remote set-url origin git@github.com:cornyhorse/forklift.git  # switch to SSH
ssh -T git@github.com                                             # test the connection
```

### Generated files in the working tree

Keep generated files out of version control, e.g. `bad_rows_*.json` in `.gitignore`.

## Quick Release Checklist

### Pre-Release
- [ ] All changes committed and pushed to the release branch; CI green
- [ ] Version updated in `pyproject.toml` and `src/forklift/__init__.py`
- [ ] `CHANGELOG.md` updated (`[Unreleased]` moved to the new version)
- [ ] Local build checked: `python -m build` and `twine check dist/*`

### GitHub Release
- [ ] Pull request merged to `main`
- [ ] Switched to `main` and pulled the latest
- [ ] Tag created from `main`: `git tag -a v0.1.4 -m "Release version 0.1.4"`
- [ ] Tag pushed: `git push origin refs/tags/v0.1.4`
- [ ] GitHub release published with release notes (this starts the PyPI workflow)

### PyPI
- [ ] Publish workflow finished green (tag check, build, tests, upload)
- [ ] Installation from PyPI verified: `pip install forklift-etl==0.1.4`

### Post-Release
- [ ] Release branch deleted locally and remotely
- [ ] Documentation updated and release communicated
- [ ] Next version planning initiated

## Best Practices

1. **Always test releases**: use TestPyPI before a manual production upload
2. **Semantic versioning**: follow SemVer strictly
3. **Document changes**: maintain `CHANGELOG.md` as you go, not at release time
4. **Tag consistently**: `v` + the version in `pyproject.toml` (e.g. `v0.1.4`)
5. **Keep the automation honest**: the publish workflow is the gate - do not bypass it for routine
   releases
6. **Keep local copies** of release artifacts until the release is verified
7. **Plan version increments** in advance

## Security Considerations

- Prefer trusted publishing (the workflow) over long-lived tokens; if you must use a token, scope it
  to this project and store it in a secret manager or environment variable
- Verify package contents before uploading (`tar -tzf` / `unzip -l`)
- Monitor dependencies for security vulnerabilities (Dependabot is enabled for pip and GitHub
  Actions)
- Consider package signing for critical releases

---

*This process guide should be updated as the project evolves and new tools/practices are adopted.*
