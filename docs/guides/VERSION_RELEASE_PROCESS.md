# Forklift Version Release Process

This guide outlines the complete process for releasing a new version of Forklift, including updating both GitHub and PyPI packages.

## Prerequisites

Before starting a release, ensure you have:

- [ ] Write access to the GitHub repository
- [ ] PyPI publishing credentials configured
- [ ] All changes merged and tested on the release branch
- [ ] Release notes prepared

## Version Release Workflow

### 1. Pre-Release Preparation

#### 1.1 Branch Management
```bash
# Ensure you're on your release branch (e.g., v0.1.4)
git checkout v0.1.4

# Ensure branch is up to date
git pull origin v0.1.4

# Verify current version in pyproject.toml
grep "version" pyproject.toml
```

#### 1.2 Version Number Strategy
Follow semantic versioning (SemVer):
- **Patch (0.1.3 → 0.1.4)**: Bug fixes, minor improvements
- **Minor (0.1.4 → 0.2.0)**: New features, backward compatible
- **Major (0.2.0 → 1.0.0)**: Breaking changes

### 2. Update Version Information

#### 2.1 Update pyproject.toml
```bash
# Edit the version field in pyproject.toml
# Change: version = "0.1.3"
# To:     version = "0.1.4"
```

#### 2.2 Update Documentation (if applicable)
- Update any version references in README.md
- Update API documentation if version-specific
- Update installation instructions if needed

#### 2.3 Create/Update CHANGELOG
Create or update CHANGELOG.md with:
- Release version and date
- New features
- Bug fixes
- Breaking changes (if any)
- Known issues

### 3. Pre-Release Testing

#### 3.1 Local Testing
```bash
# Install in development mode
pip install -e .

# Run test suite
pytest tests/

# Test package build
python -m build

# Verify package contents
tar -tzf dist/forklift_etl-0.1.4.tar.gz
```

#### 3.2 Integration Testing
- Test key functionality with real data
- Verify all x-attributes work as expected
- Test with different Python versions (if applicable)

### 4. GitHub Release Process

#### 4.1 Merge to Main Branch
```bash
# Switch to main branch
git checkout main

# Merge your release branch
git merge v0.1.4

# Push to main
git push origin main
```

#### 4.2 Create Git Tag
```bash
# Create annotated tag
git tag -a v0.1.4 -m "Release version 0.1.4"

# Push tag to remote
git push origin v0.1.4
```

#### 4.3 Create GitHub Release
1. Go to GitHub repository → Releases
2. Click "Create a new release"
3. Choose tag: `v0.1.4`
4. Release title: `Forklift v0.1.4`
5. Add release notes from CHANGELOG
6. Upload any additional assets (if needed)
7. Click "Publish release"

### 5. PyPI Release Process

#### 5.1 Clean Previous Builds
```bash
# Remove previous build artifacts
rm -rf dist/
rm -rf build/
rm -rf *.egg-info/
```

#### 5.2 Build Package
```bash
# Install build tools (if not already installed)
pip install build twine

# Build source and wheel distributions
python -m build
```

#### 5.3 Verify Build
```bash
# Check package contents
twine check dist/*

# Test upload to TestPyPI (optional but recommended)
twine upload --repository testpypi dist/*

# Install from TestPyPI to verify
pip install --index-url https://test.pypi.org/simple/ forklift-etl==0.1.4
```

#### 5.4 Upload to PyPI
```bash
# Upload to production PyPI
twine upload dist/*

# Verify on PyPI website
# Visit: https://pypi.org/project/forklift-etl/
```

### 6. Post-Release Activities

#### 6.1 Verify Installation
```bash
# Test installation from PyPI
pip install forklift-etl==0.1.4

# Verify version
python -c "import forklift; print(forklift.__version__)"
```

#### 6.2 Update Documentation
- Update any version-specific documentation
- Update installation instructions
- Update examples if API changed

#### 6.3 Communication
- Announce release on relevant channels
- Update project status/roadmap
- Close related GitHub issues/milestones

### 7. Troubleshooting

#### Common Issues and Solutions

**PyPI Upload Fails**
```bash
# Check credentials
twine upload --repository pypi dist/* --verbose

# If authentication fails, configure credentials:
# ~/.pypirc or use environment variables
```

**Version Conflicts**
- Ensure version in pyproject.toml matches git tag
- Check for existing releases with same version
- Verify semantic versioning compliance

**Build Failures**
```bash
# Check dependencies
pip install --upgrade build setuptools wheel

# Verify project structure
python -m build --verbose
```

**Git Tag Issues**
```bash
# Delete tag if needed
git tag -d v0.1.4
git push origin --delete v0.1.4

# Recreate tag
git tag -a v0.1.4 -m "Release version 0.1.4"
git push origin v0.1.4
```

## Release Checklist

### Pre-Release
- [ ] All tests passing
- [ ] Version updated in pyproject.toml
- [ ] CHANGELOG updated
- [ ] Documentation updated
- [ ] Local testing completed

### GitHub Release
- [ ] Branch merged to main
- [ ] Git tag created and pushed
- [ ] GitHub release published
- [ ] Release notes added

### PyPI Release
- [ ] Build artifacts cleaned
- [ ] Package built successfully
- [ ] Package verified with twine check
- [ ] TestPyPI upload tested (optional)
- [ ] Production PyPI upload completed
- [ ] Installation verified

### Post-Release
- [ ] Installation from PyPI verified
- [ ] Documentation updated
- [ ] Release communicated
- [ ] Next version planning initiated

## Configuration Files

### .pypirc Example
```ini
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

### Environment Variables
```bash
# Alternative to .pypirc
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=pypi-your-api-token-here
```

## Best Practices

1. **Always test releases**: Use TestPyPI before production
2. **Semantic versioning**: Follow SemVer strictly
3. **Document changes**: Maintain detailed CHANGELOG
4. **Tag consistently**: Use consistent tag naming (v0.1.4)
5. **Automate when possible**: Consider GitHub Actions for CI/CD
6. **Backup strategy**: Keep local copies of release artifacts
7. **Version planning**: Plan version increments in advance

## Security Considerations

- Use API tokens instead of passwords for PyPI
- Store credentials securely (environment variables, secret managers)
- Verify package contents before upload
- Monitor for security vulnerabilities in dependencies
- Consider package signing for critical releases

## Next Steps

After completing v0.1.4 release:
1. Plan features for v0.1.5 or v0.2.0
2. Create new development branch
3. Update project roadmap
4. Address any post-release feedback
5. Monitor usage and performance metrics

---

*This process guide should be updated as the project evolves and new tools/practices are adopted.*
