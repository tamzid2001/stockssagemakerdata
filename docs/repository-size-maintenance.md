---
title: "Repository size maintenance"
description: "Keep generated artifacts and heavyweight model assets out of Git history."
---

# Repository size maintenance

Generated screenshots, compiled application bundles, package installations, forecast artifacts, and mobile release binaries must not be committed to this repository. Store QA screenshots and large run outputs as short-lived CI artifacts instead.

The current branch removes tracked `quantura_site/qa_screens/` and `quantura_site/docs/screenshots/` assets and ignores both paths. Existing mobile release binaries were already removed from the current tree, but their blobs remain in Git history. A normal commit cannot delete historical objects.

## Optional coordinated history cleanup

The largest historical objects are obsolete Android `.aab`/release binaries of roughly 16–17 MB each. Reclaiming their remote Git storage requires a one-time history rewrite with `git filter-repo` (or an equivalent reviewed tool), followed by a protected force-push of every rewritten branch/tag and fresh clones for collaborators.

That operation must be scheduled as repository maintenance because it changes commit IDs. Before running it:

1. Merge and deploy all active work.
2. Pause branch updates and notify collaborators.
3. Create a recoverable repository backup or archive.
4. Enumerate all branches/tags that must remain.
5. Remove only verified generated release paths/blobs.
6. Validate the rewritten repository, tags, build, and deployment.
7. Force-push with lease under an agreed maintenance window.
8. Ask GitHub Support to run repository garbage collection if remote storage does not fall after unreachable-object retention.

Do not perform this rewrite as part of an ordinary feature merge.
