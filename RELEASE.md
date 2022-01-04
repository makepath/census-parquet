## Release process

### Preparation
- Create a new branch containing the following changes:
  - Update version number in setup.py
  - Update CHANGELOG.md with new version number and list of changes extracted from `git log`.
- Commit changes
- Check you can build the `sdist` and binary `wheel` using
  ```bash
  python setup.py sdist bdist_wheel
  ```
  You may need to `pip install wheel` first. It should create a `.tar.gz` file and a `.whl` file in the `dist` directory. Check these are OK manually.
- `git push` the branch.
- Submit the branch as a PR to the `master` branch.
- If the CI passes OK, merge the PR.

### Tag release
- To sign the release you need a GPG key registered with your github account. See
https://docs.github.com/en/authentication/managing-commit-signature-verification
- Create new tag, with the correct version number, using:
  ```bash
  git tag -a v0.1.2 -s -m "Version 0.1.2"
  git push --tags
  ```

### PyPI packages
- These are automatically built and uploaded to PyPI via a github action when a new tag is pushed to the github repo.
- Check that both an sdist (`.tar.gz` file) and wheel (`.whl` file) are available on PyPI at https://pypi.org/project/census-parquet
- Check you can install the new version in a new virtual environment using `pip install census-parquet`.

### github release notes
- Convert the tag into a release on github:
  - On the right-hand side of the github repo, click on `Releases`.
  - Click on `Draft a new release`.
  - Select the correct tag, and enter the title and description by copying and pasting from the CHANGELOG.md.
  - Click `Publish release`.
