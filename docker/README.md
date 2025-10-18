# Forklift ETL Docker Setup

This directory contains Docker configuration files for the Forklift ETL project.

## Files

- `Dockerfile` - Multi-stage Docker build configuration
- `Makefile` - Convenient commands for building and running Docker images
- `.dockerignore` - Files to exclude from Docker build context (moved here for organization)

## Quick Start

### Build Images

```bash
# From project root
make -f docker/Makefile docker-build-prod  # Production image
make -f docker/Makefile docker-build-dev   # Development image
```

### Run Tests

```bash
# Run tests in container
make -f docker/Makefile docker-test
```

### Development

```bash
# Start interactive development container
make -f docker/Makefile docker-dev
```

## Docker Images

### Production Image (`forklift-etl:latest`)
- Lightweight runtime image
- All dependencies pre-installed
- Optimized for CI/CD and production use
- Default command runs pytest

### Development Image (`forklift-etl:dev`)
- Includes additional development tools
- Jupyter notebook support
- Interactive shell access
- Exposes port 8888 for Jupyter

## GitHub Container Registry

Images are automatically built and pushed to `ghcr.io/cornyhorse/forklift` when:
- Code is pushed to `main` or `develop` branches
- Pull requests are created
- Tags are pushed

### Available Tags
- `latest` - Latest stable build from main branch
- `ci` - Optimized for CI/CD usage
- `dev` - Development image with extra tools
- Version tags (e.g., `v1.0.0`) - Released versions

## Manual Build and Push

```bash
# Build and tag for registry
docker build --target production -t ghcr.io/cornyhorse/forklift:latest -f docker/Dockerfile .

# Push to registry (requires authentication)
docker push ghcr.io/cornyhorse/forklift:latest
```

## Local Development Workflow

1. **Build the development image:**
   ```bash
   make -f docker/Makefile docker-build-dev
   ```

2. **Start development container:**
   ```bash
   make -f docker/Makefile docker-dev
   ```

3. **Run tests:**
   ```bash
   # Inside container
   python -m pytest tests/ -v
   ```

4. **Start Jupyter (if needed):**
   ```bash
   # Inside container
   jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
   ```

## CI/CD Integration

The GitHub Actions workflows automatically use the pre-built container images to speed up CI execution:

- **Fast Test Workflow**: Uses `ghcr.io/cornyhorse/forklift:ci`
- **Test Suite Workflow**: Uses `ghcr.io/cornyhorse/forklift:ci`

This eliminates the need to install dependencies from scratch on every CI run, significantly reducing build times.
