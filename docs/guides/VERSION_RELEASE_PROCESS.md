- [ ] Package verified: `twine check dist/*`
- [ ] Uploaded to PyPI: `twine upload dist/*`
# Forklift Version Release Process
### Cleanup
- [ ] Feature branch deleted locally and remotely
- [ ] Installation verified from PyPI

Before starting a release, ensure you have:
## Configuration
- [ ] Write access to the GitHub repository
### PyPI Credentials
Use API tokens in `~/.pypirc`:
- [ ] All changes merged and tested on the release branch
- [ ] Release notes prepared
index-servers = pypi testpypi
### 1. Pre-Release Preparation

#### 1.1 Branch Management
```bash
# Work on your feature/release branch (e.g., v0.1.4)
git checkout v0.1.4

# Ensure branch is up to date
git pull origin v0.1.4

# Verify current version in pyproject.toml
## Key Lessons Learned

1. **Always create PRs first** - Never tag directly from feature branches
2. **Use explicit git refs** - Avoid ambiguity between branches and tags with same names
3. **SSH authentication** - Easier than HTTPS for frequent operations
4. **Proper gitignore** - Keep generated files out of version control
5. **Clean up after releases** - Remove feature branches once merged and tagged
# Create annotated tag
git tag -a v0.1.4 -m "Release version 0.1.4"

*This process has been refined based on real-world experience and common issues encountered during releases.*
```

### 3. Common Issues and Solutions

#### 3.1 "src refspec matches more than one" Error
This happens when you have both a branch and tag with the same name.

**Solution:**
```bash
# Push tag explicitly using full reference
git push origin refs/tags/v0.1.4

# Delete branch explicitly using full reference (if needed)
git push origin --delete refs/heads/v0.1.4
```

#### 3.2 SSH vs HTTPS Authentication
**Switch to SSH for easier authentication:**
```bash
# Check current remote
git remote -v

# Switch to SSH
git remote set-url origin git@github.com:cornyhorse/forklift.git

# Test connection
ssh -T git@github.com
```

#### 3.3 Gitignore for Generated Files
Add patterns for files that shouldn't be tracked:
```bash
# Add to .gitignore
bad_rows_*.json
```

### 4. GitHub Release Process

#### 4.1 Create GitHub Release
1. Go to GitHub repository → Releases
2. Click "Create a new release"
3. Choose tag: `v0.1.4` (should already exist)
4. Release title: `v0.1.4`
5. Add comprehensive release notes
6. Click "Publish release"

### 5. PyPI Release Process

#### 5.1 Clean and Build
```bash
# Remove previous build artifacts
rm -rf dist/ build/ *.egg-info/

# Install/upgrade build tools
pip install --upgrade build twine

# Build package
python -m build

# Verify build
twine check dist/*
```

#### 5.2 Upload to PyPI
```bash
# Test upload (optional but recommended)
twine upload --repository testpypi dist/*

# Upload to production PyPI
twine upload dist/*
```

### 6. Post-Release Cleanup

#### 6.1 Clean Up Branches
```bash
# Delete local feature branch
git branch -d v0.1.4

# Delete remote feature branch (use explicit reference if needed)
git push origin --delete refs/heads/v0.1.4
```

#### 6.2 Verify Release
```bash
# Test installation from PyPI
pip install forklift-etl==0.1.4

# Verify version
python -c "import forklift; print(forklift.__version__)"
```

## Quick Release Checklist

### Pre-Release
- [ ] All changes committed and pushed to feature branch
- [ ] Version updated in pyproject.toml (if needed)
- [ ] Release notes prepared

### GitHub Release
- [ ] PR created and merged to main
- [ ] Switched to main branch and pulled latest
- [ ] Git tag created from main: `git tag -a v0.1.4 -m "Release version 0.1.4"`
- [ ] Tag pushed: `git push origin refs/tags/v0.1.4` (use explicit ref if conflicts)
- [ ] GitHub release created with release notes

### PyPI Release (if applicable)
- [ ] Build artifacts cleaned: `rm -rf dist/ build/ *.egg-info/`
- [ ] Package built: `python -m build`

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
