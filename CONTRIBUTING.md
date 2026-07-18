# Contributing

## Code of Conduct

This project and everyone participating in it is governed by a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to [quack@duckdb.org](mailto:quack@duckdb.org).

## **Did you find a bug?**

* **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/duckdb/duckdb/issues).
* If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/duckdb/duckdb/issues/new/choose). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

## **Did you write a patch that fixes a bug?**

* Great!
* If possible, add a unit test case to make sure the issue does not occur again.
* Make sure you run the code formatter (`make format-fix`).
* Open a new GitHub pull request with the patch.
* Ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable.

## Outside Contributors

* Discuss your intended changes with the core team on Github
* Announce that you are working or want to work on a specific issue
* Avoid large pull requests - they are much less likely to be merged as they are incredibly hard to review

## Pull Requests

* Do not commit/push directly to the main branch. Instead, create a fork and file a pull request.
* When maintaining a branch, merge frequently with the main.
* When maintaining a branch, submit pull requests to the main frequently.
* If you are working on a bigger issue try to split it up into several smaller issues.
* Please do not open "Draft" pull requests and ensure that you run CI on your fork + branch first before opening a PR (see CI section below). GH actions is free for public forks of open source projects.
* We reserve full and final discretion over whether or not we will merge a pull request. Adhering to these guidelines is not a complete guarantee that your pull request will be merged.
* Please refrain from opening pull requests to fix minor typos.

## CI for pull requests

* Pull requests will need to pass all continuous integration checks before merging.
* When all checks look good on your local CI, you can open a PR to run the CI on the main DuckDB repo. Submitting changes to an open pull request will move it to 'Draft' state. In that case you can mark it as "Ready for Review" once you've applied all fixes and it passes in your fork to run the public CI again ('ready for review', via the Web UI button on the bottom right).
* Note that occasionally CI failures may be unrelated. You should check whether it's related to your changes (because if it is, that means your changes are breaking something). Otherwise, you should 1) remember to merge with main frequently and run make format-fix (sometimes you need to run generate-files) 2) check if other PR CI's are failing on the same tests (that's usually a giveaway that it's a temporary problem with the CI) and generally 3) investigate that there is no overlap between your changes and the breaking CI.

## Nightly CI

* Packages creation and long running tests will be performed during a nightly run
* On your fork you can trigger long running tests (NightlyTests.yml) for any branch following information from https://docs.github.com/en/actions/using-workflows/manually-running-a-workflow#running-a-workflow

## C API V2 development

DuckDB carries a C API V2. The design reference is `api_spec/C_API_V2.md`; the standing invariants and a map of the V2 directories are in the "DuckDB C API V2" section of `AGENTS.md`. This section covers setup, regeneration, building, and testing. Directory-local `AGENTS.md` files carry the authoring conventions: `api_spec/AGENTS.md` (editing the spec), `src/main/capi_v2/AGENTS.md` (writing bridge implementations), `test/api/capi_v2/AGENTS.md` (writing tests).

### Prerequisites and setup

Install [Astral uv](https://docs.astral.sh/uv/getting-started/installation/) (the Python package manager used by the generator and the formatter):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Provision the root virtual environment, which installs `capigen` (from PyPI, pinned) and the formatter runtime (clang-format, black, ...) pinned to the versions CI uses. `cmake-format` is deliberately not in the root venv; it runs only inside its pre-commit hook's isolated environment.

```bash
uv sync --group dev
```

You also need the standard DuckDB build dependencies: a C++17 compiler, CMake, and Ninja (optional but recommended).

### Pre-commit hook

`.pre-commit-config.yaml` owns the regeneration and formatting pipeline:

- **`capi-v2-regen`** fires when any `api_spec/v2/**/*.yaml` is staged; runs `scripts/capi_v2_regen.sh` to regenerate the V2 header and stubs.
- **`capi-v1-regen`** fires when any `api_spec/v1/**/*.yaml` or the extension seed (`api_spec/v1/extension/duckdb_extension.h.in`) is staged; runs `scripts/capi_v1_regen.sh` to regenerate the V1 and extension headers.
- **`duckdb-format`** runs `scripts/format.py` on staged C/C++/Python/test changes and on files the regen hooks produce. The manual-stage `duckdb-format-check` runs the full-tree `--all --check` pass in CI.
- **`cmake-format`** (from `cheshirekow/cmake-format-precommit`) formats `CMakeLists.txt` and `*.cmake` in its own isolated venv pinned to Python 3.12.
- **`check-yaml` / `yamlfmt`** validate and format the `api_spec/` YAML.

One-time setup per clone (alongside `uv sync --group dev`):

```bash
uv run pre-commit install
```

When a hook modifies a staged file, pre-commit aborts the commit and prints the changed files; re-`git add` them and commit again. To bypass for a single commit (not recommended), use `git commit --no-verify`.

### Regenerating the header and stubs

The V2 header (`src/include/duckdb_v2.h`) and the bridge stubs (`src/main/capi_v2/capi_v2_stubs.cpp`) are committed and generated from the spec. After editing the YAML:

```bash
./scripts/capi_v2_regen.sh
```

This runs both capigen adapters (`c` for the header, `bridge` for the stubs) and formats the output. The `capi-v2-regen` pre-commit hook runs it automatically when you stage a spec change, so committing without a manual run also works. If you add a new bridge implementation file to `src/main/capi_v2/`, add it to that directory's `CMakeLists.txt`.

### Building and testing

The V2 capi compiles into the standard DuckDB build:

```bash
make debug                                 # full build (or: make release)
./build/debug/test/unittest "[capi_v2]"    # V2 C API bridge tests
./build/debug/test/unittest "[cpp_api]"    # stable C++ API tests
./build/debug/test/unittest "[capi]"       # V1 regression (must stay green)
```

Common gotchas:

- **YAML edits require regeneration.** Forgetting to regenerate shows up as drift in `git status` and fails the CI `git diff --exit-code` check.
- **Hand-written bridges are not overwritten by regeneration.** A renamed or deleted spec function leaves its orphaned implementation in the `.cpp` until you remove it manually.
- **Error codes are 32-bit** (`(group_id << 16) | code`). Use the generated `DUCKDB_V2_ERROR_*` macro, never the numeric value.
- **New primitives are declared in `api_spec/v2/metadata.yaml`** first, with their C ABI type under `c_type`.

### CI

`.github/workflows/v2-capi.yml` runs on every push to `main` and on PRs, as two jobs:

- **`format`** provisions the root venv with `uv sync --group dev`, runs `pre-commit run --all-files` and the manual full-tree `scripts/format.py --all --check`, then `git diff --exit-code` to fail if the committed headers or stubs are out of sync with `api_spec/`.
- **`build`** builds with `make relassert` (`FORCE_DEBUG=1 FORCE_ASSERT=1`, RelWithDebInfo plus ASan/UBSan/LSan and the slow verifiers), then runs `make unittest_relassert T="[capi_v2],[capi]"`, plus the SQL `SET` regression suites that exercise the same `PhysicalSet::ApplyVariable` path the V2 `*_option_set` bridges use.

A second workflow, `.github/workflows/sqllogic-cpp-api.yml`, runs nightly (and on demand). It runs the full sqllogic suite through the stable C++ API executor and diffs it against the internal `ClientContext::Query` path across the configuration matrix and platforms, failing only on tests that regress under the C-API runner but pass under the internal one.

## Building

* To build the project, run `make`.
* To build the project for debugging, run `make debug`.
* For parallel builds, you can use the [Ninja](https://ninja-build.org/) build system: `GEN=ninja make`.
  * The default number of parallel processes can lock up the system depending on the CPU-to-memory ratio. If this happens, restrict the maximum number of build processes: `CMAKE_BUILD_PARALLEL_LEVEL=4 GEN=ninja make`.
  * Without using Ninja, build times can still be reduced by setting `CMAKE_BUILD_PARALLEL_LEVEL=$(nproc)`.
* To speed up rebuilds, install [ccache](https://ccache.dev/). The build system will automatically detect and use it if available.

## Testing

* Unit tests can be written either using the sqllogictest framework (`.test` files) or in C++ directly. We **strongly** prefer tests to be written using the sqllogictest framework. Only write tests in C++ if you absolutely need to (e.g. when testing concurrent connections or other exotic behavior).
* Documentation for the testing framework can be found [here](https://duckdb.org/dev/testing).
* Write many tests.
* Test with different types, especially numerics, strings and complex nested types.
* Try to test unexpected/incorrect usage as well, instead of only the happy path.
* `make unit` runs the **fast** unit tests (~one minute), `make allunit` runs **all** unit tests (~one hour).
* Make sure **all** unit tests pass before sending a PR.
* Slower tests should be added to the **all** unit tests. You can do this by naming the test file `.test_slow` in the sqllogictests, or by adding `[.]` after the test group in the C++ tests.
* Look at the code coverage report of your branch and attempt to cover all code paths in the fast unit tests. Attempt to trigger exceptions as well. It is acceptable to have some exceptions not triggered (e.g. out of memory exceptions or type switch exceptions), but large branches of code should always be either covered or removed.
* DuckDB uses GitHub Actions as its continuous integration (CI) tool. You also have the option to run GitHub Actions on your forked repository. For detailed instructions, you can refer to the [GitHub documentation](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository). Before running GitHub Actions, please ensure that you have all the Git tags from the duckdb/duckdb repository. To accomplish this, execute the following commands `git fetch <your-duckdb/duckdb-repo-remote-name> --tags` and then 
`git push --tags` These commands will fetch all the git tags from the duckdb/duckdb repository and push them to your forked repository. This ensures that you have all the necessary tags available for your GitHub Actions workflow. 

## Formatting

* Use tabs for indentation, spaces for alignment.
* Lines should not exceed 120 columns.
* To make sure the formatting is consistent, please use version 11.0.1, installable through `python3 -m pip install clang-format==11.0.1` or `pipx install clang-format==11.0.1`.
* `clang_format` and `black` enforce these rules automatically, use `make format-fix` to run the formatter.
* The project also comes with an [`.editorconfig` file](https://editorconfig.org/) that corresponds to these rules.

## C++ Guidelines

* Do not use `malloc`, prefer the use of smart pointers. Keywords `new` and `delete` are a code smell.
* Strongly prefer the use of `unique_ptr` over `shared_ptr`, only use `shared_ptr` if you **absolutely** have to.
* Use `const` whenever possible.
* Do **not** import namespaces (e.g. `using std`).
* All functions in source files in the core (`src` directory) should be part of the `duckdb` namespace.
* When overriding a virtual method, avoid repeating virtual and always use `override` or `final`.
* Use `[u]int(8|16|32|64)_t` instead of `int`, `long`, `uint` etc. Use `idx_t` instead of `size_t` for offsets/indices/counts of any kind.
* Prefer using references over pointers as arguments.
* Use `const` references for arguments of non-trivial objects (e.g. `std::vector`, ...).
* Use C++11 for loops when possible: `for (const auto& item : items) {...}`
* Use braces for indenting `if` statements and loops. Avoid single-line if statements and loops, especially nested ones.
* **Class Layout:** Start out with a `public` block containing the constructor and public variables, followed by a `public` block containing public methods of the class. After that follow any private functions and private variables. For example:
    ```cpp
    class MyClass {
    public:
    	MyClass();

    	int my_public_variable;

    public:
    	void MyFunction();

    private:
    	void MyPrivateFunction();

    private:
    	int my_private_variable;
    };
    ```
* Avoid [unnamed magic numbers](https://en.wikipedia.org/wiki/Magic_number_(programming)). Instead, use named variables that are stored in a `constexpr`.
* [Return early](https://medium.com/swlh/return-early-pattern-3d18a41bba8). Avoid deep nested branches.
* Do not include commented out code blocks in pull requests.
* Always use `enum class`, never use C-style `enum`. Assign an explicit storage layout (`: uint8_t`) only if the enum is intended to be serialized. 

## Error Handling

* Use exceptions **only** when an error is encountered that terminates a query (e.g. parser error, table not found). Exceptions should only be used for **exceptional** situations. For regular errors that do not break the execution flow (e.g. errors you **expect** might occur) use a return value instead.
* Try to add test cases that trigger exceptions. If an exception cannot be easily triggered using a test case then it should probably be an assertion. This is not always true (e.g. out of memory errors are exceptions, but are very hard to trigger).
* Use `D_ASSERT` to assert. Use **assert** only when failing the assert means a programmer error. Assert should never be triggered by user input. Avoid code like `D_ASSERT(a > b + 3);` without comments or context.
* Assert liberally, but make it clear with comments next to the assert what went wrong when the assert is triggered.

## Naming Conventions

* Choose descriptive names. Avoid single-letter variable names.
* Files: lowercase separated by underscores, e.g., abstract_operator.cpp
* Types (classes, structs, enums, typedefs, using): CamelCase starting with uppercase letter, e.g., BaseColumn
* Variables: lowercase separated by underscores, e.g., chunk_size
* Functions: CamelCase starting with uppercase letter, e.g., GetChunk
* Avoid `i`, `j`, etc. in **nested** loops. Prefer to use e.g. **column_idx**, **check_idx**. In a **non-nested** loop it is permissible to use **i** as iterator index.
* These rules are partially enforced by `clang-tidy`.

## Generative AI Policy

Please do not submit pull requests generated by AI (LLMs).
Reviewing such PRs puts a considerable burden on the maintainers.
