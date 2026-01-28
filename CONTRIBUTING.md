# Contributing to Space Invaders Consumer

We welcome contributions to the Space Invaders Consumer project! This document provides guidelines and information for contributors.

## 🤝 How to Contribute

### Types of Contributions

- **Bug Reports**: Help us identify and fix issues
- **Feature Requests**: Suggest new functionality or improvements
- **Code Contributions**: Submit bug fixes, features, or optimizations
- **Documentation**: Improve or expand documentation
- **Testing**: Add or improve test coverage

## 🚀 Getting Started

### Development Setup

1. **Fork the Repository**
   ```bash
   # Fork on GitHub, then clone your fork
   git clone https://github.com/your-username/invaders.consumer.git
   cd invaders.consumer
   ```

2. **Install Dependencies**
   ```bash
   npm install
   # or
   yarn install
   ```

3. **Environment Setup**
   ```bash
   # Copy example environment file
   cp .env.example .env
   
   # Configure your local environment variables
   # You'll need access to:
   # - PostgreSQL database
   # - RabbitMQ instance
   # - Pinata JWT token (for IPFS)
   ```

4. **Database Setup**
   ```bash
   # Run database migrations
   psql $DATABASE_URL -f migrations/001_create_flashcastr_ipfs_flashes.sql
   psql $DATABASE_URL -f migrations/002_add_ipfs_cid_to_flashes.sql
   ```

5. **Verify Setup**
   ```bash
   # Build the project
   npm run build
   
   # Run in test mode
   npm test
   ```

### Development Workflow

1. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Your Changes**
   - Follow the coding standards outlined below
   - Write tests for new functionality
   - Update documentation as needed

3. **Test Your Changes**
   ```bash
   # Run performance checks
   npm run performance-check
   
   # Run health checks
   npm run health-check
   
   # Test the build
   npm run build
   ```

4. **Commit Your Changes**
   ```bash
   git commit -m "feat: add your feature description"
   ```

5. **Push and Create Pull Request**
   ```bash
   git push origin feature/your-feature-name
   # Create pull request on GitHub
   ```

## 📝 Coding Standards

### TypeScript Guidelines

- Use TypeScript for all new code
- Prefer `const` over `let`, avoid `var`
- Use meaningful variable and function names
- Add type annotations for public APIs
- Use interfaces for object shapes

```typescript
// Good
interface FlashProcessingResult {
  success: boolean;
  ipfsHash?: string;
  error?: string;
}

async function processFlash(flash: Flash): Promise<FlashProcessingResult> {
  // Implementation
}

// Avoid
function process(data: any): any {
  // Implementation
}
```

### Code Style

- Use 2 spaces for indentation
- Use semicolons at the end of statements
- Use single quotes for strings
- Maximum line length: 100 characters
- Use trailing commas in multiline objects/arrays

### Error Handling

- Always handle errors appropriately
- Use specific error types when possible
- Log errors with sufficient context
- Don't swallow errors silently

```typescript
// Good
try {
  const result = await processImage(imageData);
  return result;
} catch (error) {
  console.error(`Image processing failed for flash ${flashId}:`, error);
  throw new Error(`Image processing failed: ${error.message}`);
}

// Avoid
try {
  const result = await processImage(imageData);
  return result;
} catch (error) {
  // Silent failure
}
```

### Performance Considerations

- Use batch operations when possible
- Implement proper rate limiting
- Monitor memory usage
- Use circuit breakers for external calls
- Prefer streams for large data processing

## 🧪 Testing

### Test Categories

1. **Unit Tests**: Test individual functions and modules
2. **Integration Tests**: Test component interactions
3. **Performance Tests**: Verify performance requirements
4. **End-to-End Tests**: Test complete workflows

### Writing Tests

- Write tests for all new functionality
- Aim for high test coverage
- Use descriptive test names
- Mock external dependencies
- Test error conditions

```typescript
describe('FlashProcessor', () => {
  it('should successfully process a valid flash image', async () => {
    // Arrange
    const mockFlash: Flash = {
      flash_id: 123,
      img: '/path/to/image.jpg',
      // ... other properties
    };

    // Act
    const result = await processor.processFlash(mockFlash);

    // Assert
    expect(result.success).toBe(true);
    expect(result.ipfsHash).toBeDefined();
  });

  it('should handle network errors gracefully', async () => {
    // Test error scenarios
  });
});
```

## 📋 Pull Request Guidelines

### Before Submitting

- [ ] Code follows the style guidelines
- [ ] Tests have been added/updated
- [ ] Documentation has been updated
- [ ] Performance impact has been considered
- [ ] Changes have been tested locally

### Pull Request Template

```markdown
## Description
Brief description of the changes made.

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Performance tests run
- [ ] Manual testing performed

## Checklist
- [ ] Code follows the style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests added and passing
```

## 🐛 Bug Reports

### Before Reporting

1. Check if the issue already exists
2. Try to reproduce with the latest version
3. Gather relevant information (logs, environment, etc.)

### Bug Report Template

```markdown
## Bug Description
A clear description of what the bug is.

## Environment
- OS: [e.g., Ubuntu 20.04]
- Node.js version: [e.g., 19.9.0]
- Package version: [e.g., 1.0.0]

## Steps to Reproduce
1. Go to '...'
2. Click on '...'
3. Scroll down to '...'
4. See error

## Expected Behavior
What you expected to happen.

## Actual Behavior
What actually happened.

## Additional Context
Add any other context about the problem here.
```

## 💡 Feature Requests

### Feature Request Template

```markdown
## Feature Description
A clear description of the feature you'd like to see.

## Problem Statement
What problem does this solve?

## Proposed Solution
How would you implement this feature?

## Alternatives Considered
Other approaches you've considered.

## Additional Context
Any other relevant information.
```

## 🏗️ Architecture Guidelines

### Performance Considerations

- Always use batch operations for database updates
- Implement proper rate limiting for external APIs
- Use circuit breakers for fault tolerance
- Monitor memory usage and implement cleanup
- Prefer streams for large data processing

### Security Guidelines

- Never commit secrets or credentials
- Use environment variables for configuration
- Validate all input data
- Implement proper error handling that doesn't leak sensitive information
- Use HTTPS for all external communications

### Monitoring and Observability

- Add appropriate logging for new features
- Include performance metrics where relevant
- Implement health checks for new services
- Update monitoring dashboards as needed

## 🔄 Release Process

### Versioning

We use [Semantic Versioning](https://semver.org/):
- `MAJOR.MINOR.PATCH`
- Major: Breaking changes
- Minor: New features (backwards compatible)
- Patch: Bug fixes (backwards compatible)

### Release Checklist

- [ ] Update version in package.json
- [ ] Update CHANGELOG.md
- [ ] Create release notes
- [ ] Tag the release
- [ ] Deploy to staging
- [ ] Run full test suite
- [ ] Deploy to production

## 🆘 Getting Help

### Communication Channels

- **GitHub Issues**: For bug reports and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Documentation**: Check [README.md](README.md) and [PERFORMANCE_IMPROVEMENTS.md](PERFORMANCE_IMPROVEMENTS.md)

### Questions?

If you have questions about contributing, please:
1. Check the existing documentation
2. Search through GitHub issues and discussions
3. Create a new discussion if needed

## 🙏 Recognition

All contributors will be recognized in our contributors list. We appreciate:
- Code contributions
- Documentation improvements
- Bug reports and testing
- Feature suggestions
- Community support

Thank you for contributing to Space Invaders Consumer! 🚀