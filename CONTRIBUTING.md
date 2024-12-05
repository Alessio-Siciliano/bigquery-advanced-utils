# Contributing to **bigquery-advanced-utils**

Thank you for considering contributing to **bigquery-advanced-utils**! Your contributions are essential to improve and expand this project. We welcome issues, bug reports, new features, and code improvements.

Please take a moment to review this guide to help you get started.

## How to Contribute

### 1. **Report Bugs or Request Features**
If you find a bug or have a feature request, please [open an issue](https://github.com/Alessio-Siciliano/bigquery-advanced-utils/issues) and provide as much detail as possible:
- Describe the problem clearly.
- Provide steps to reproduce the issue (if applicable).
- Add any relevant error messages or logs.

### 2. **Fork the Repository**
To start contributing, fork the repository by clicking the "Fork" button in the top-right corner of this page. This creates your own copy of the project.

### 3. **Create a New Branch**
Create a new branch for your feature or bug fix:
```bash
git checkout -b feature/your-feature-name
```
or for bug fixes:
```bash
git checkout -b bugfix/issue-number
```

### 4. **Make Changes**
Make your changes in the codebase. Make sure to:
- Follow the **PEP 8** style guide.
- Ensure the code is properly formatted (use tools like **Black**).
- Add tests (if applicable).
- Make sure all existing tests pass.
- Run **PyLint** to check the quality of your code.

### 5. **Commit Your Changes**
Write clear and concise commit messages. For example:
```bash
git commit -m "Add a new query builder utility"
```
### 6. **Push Your Changes**
Once you’re ready, push your changes back to your fork:
```bash
git push origin feature/your-feature-name`
```
### 7. **Submit a Pull Request**
Go to the [Pull Requests](https://github.com/Alessio-Siciliano/bigquery-advanced-utils/pulls) page of the repository and submit a pull request (PR). Be sure to provide a description of what you’ve done and why it’s beneficial to the project.

Make sure your PR follows these rules:
- It should reference the issue it resolves, if applicable.
- It should be based on the most recent `main` branch.
- Include any relevant documentation updates if necessary.

## Code Style Guidelines

- **Python version**: This project uses Python 3.10+.
- **Formatting**: We use **Black** for code formatting. Please run Black on your code before submitting a pull request.
- **Linting**: All code should pass **PyLint**. Please ensure your code has no warnings or errors before submitting your PR.
- **Docstrings**: All functions and classes should have docstrings explaining their purpose and usage.

## Testing

We aim to maintain a high level of test coverage. Please write tests for any new functionality or bug fixes, and ensure that all tests pass before submitting your PR.

To run tests:
```bash
pytest
```
## License

By contributing, you agree that your contributions will be licensed under the **GNU General Public License** v3.

## Thank You!

Thank you for your contributions! Every pull request helps make **bigquery-advanced-utils** a better project. We look forward to your improvements!
