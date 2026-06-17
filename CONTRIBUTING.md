# Contributing to MultiStorage

Thank you for your interest in contributing to MultiStorage! We welcome contributions from everyone to help make this project better.

## How Can I Contribute?

### Reporting Bugs
- Search the issue tracker to see if the bug has already been reported.
- If not, create a new issue with a clear title and a detailed description, including steps to reproduce the bug.
- Include information about your environment (OS, Go version, FUSE version).

### Suggesting Enhancements
- Open an issue to discuss your idea before starting work.
- Provide a clear description of the enhancement and why it would be useful.

### Pull Requests
1. Fork the repository.
2. Create a new branch for your feature or bug fix: `git checkout -b feature/your-feature-name`.
3. Make your changes.
4. Ensure your code follows the existing style and matches the patterns in the codebase.
5. Run existing tests (if any) and ideally add new ones for your changes.
6. Commit your changes with descriptive commit messages.
7. Push your branch to your fork: `git push origin feature/your-feature-name`.
8. Open a Pull Request against the `main` branch.

## Development Guidelines

- **Go Version**: Use Go 1.22+.
- **FUSE**: Ensure you have FUSE developers headers installed if you are working on the client or server FUSE components.
- **Provider Interface**: If adding a new storage provider, implement the `Provider` interface found in [server/provider.go](server/provider.go).
- **Protocol**: Any changes to the communication between client and server should be reflected in [protocol.go](protocol.go) and [client/protocol.go](client/protocol.go).

## Coding Standards
- Follow standard Go formatting (`go fmt`).
- Write clear, documented code.
- Keep functions focused and modular.

## License
By contributing, you agree that your contributions will be licensed under the project's MIT License.
