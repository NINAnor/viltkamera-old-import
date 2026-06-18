# viltkamera-old-import

A tool to import old projects, timeseries and images exported by the [export tool](https://github.com/NINAnor/viltkamera-old-export) in the new database.

Once the migration is completed the reposiory will be archived.

## Setup
Install `uv`: https://docs.astral.sh/uv/getting-started/installation/

```bash
git init
uv sync --dev
git add .
git commit -m "Initial commit"
uv run prek install # optional
```


### Run
To execute your software you have two options:

**Option 1: Direct execution**
```bash
uv run main.py
```

**Option 2: Run as installed package**
```bash
uvx --from . viltkamera_old_import
```

### Development
Just run `uv run main.py` and you are good to go!

### Update from template
To update your project with the latest changes from the template, run:
```bash
uvx --with copier-template-extensions copier update --trust
```

You can keep your previous answers by using:
```bash
uvx --with copier-template-extensions copier update --trust --defaults
```

### (Optional) prek
prek is a fast, Rust-based tool for managing git hooks (100% compatible with pre-commit). It helps ensure code quality by running checks every time you make a commit.

First, install prek:
```bash
uv tool install prek
```

If you have installed the git hooks with `pre-commit` (template version 0.2.6 and older), remove them before installing the ones provided by prek:

```
pre-commit uninstall
```

Then install git hooks:
```bash
prek install
```

To run prek on all files:
```bash
prek run --all-files
```

### How to install a package
Run `uv add <package-name>` to install a package. For example:
```bash
uv add requests
```

#### Visual studio code
If you are using visual studio code install the recommended extensions


### Tools installed
- uv
- prek (optional)

#### What is an environment variable? and why should I use them?
Environment variables are variables that are not populated in your code but rather in the environment
that you are running your code. This is extremely useful mainly for two reasons:
- security, you can share your code without sharing your passwords/credentials
- portability, you can avoid using hard-coded values like file-system paths or folder names

you can place your environment variables in a file called `.env`, the `main.py` will read from it. Remember to:
- NEVER commit your `.env`
- Keep a `.env.example` file updated with the variables that the software expects
