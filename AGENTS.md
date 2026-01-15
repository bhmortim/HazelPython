Since the provided code snippets only contain a standard Apache 2.0 License and a nearly empty README, there is limited information on the specific Python logic or advanced language features used in this project. However, based on the file structure and the naming convention of the project (**HazelPython**), the following style guide can be established:

# HazelPython Coding Style Guide

## 1. Project Metadata & Licensing
*   **License Header**: Every repository must include the full text of the Apache License 2.0 in a root-level `LICENSE` file.
*   **Attribution**: Any derivative works or contributions must maintain copyright notices as specified in Section 4 of the Apache License.

## 2. Documentation
*   **README Structure**: Every project must contain a `README.md` file. 
*   **Header Style**: Use ATX-style headers (e.g., `# HazelPython`) rather than Setext-style headers (`===`).

## 3. Naming Conventions
*   **Project Naming**: Use PascalCase for the project name (e.g., `HazelPython`) when referring to it in documentation, but use lowercase or snake_case for the underlying package name (standard Python PEP 8 convention).

## 4. Repository Structure
*   **Core Files**: The root directory should strictly separate legal documentation (`LICENSE`), project overview (`README.md`), and source code.
*   **New Features**: In the absence of a `setup.py`, the project likely leverages modern Python packaging (e.g., `pyproject.toml` using `flit`, `poetry`, or `hatch`), though this is not explicitly shown in the provided snippet.

***

*Note: As the provided source was limited to legal and markdown boilerplate, standard PEP 8 conventions apply to all Python logic until specific project overrides are introduced.*