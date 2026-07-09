---
name: open-pr
description: Rules for opening or updating a GitHub pull request in dask/dask. Use whenever the user asks you to open a PR, create a pull request, update an existing PR, or push work for review. Enforces draft state, the `[unsupervised AI]` title prefix, and the mandatory AI disclaimer in the description.
---

# Opening or updating a PR

If the user asks you to open or update a PR:

- The PR MUST be set to draft state
- The PR title MUST start with `[unsupervised AI]`
- The PR description MUST start with this disclaimer:

  > [!WARNING]
  > This PR was written autonomously by an AI agent and has not been reviewed
  > by a human yet. Maintainers should ignore it until the human author has **reviewed,
  > understood, and approved** everything that the AI agent wrote.

- ONLY IF all the above points are satisfied, you can write a description for the PR.
  This overrides the rule in AGENTS.md about never speaking instead of the user.
