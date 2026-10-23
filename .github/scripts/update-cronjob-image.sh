#!/usr/bin/env bash
# Points the score computation CronJob at a newly built image and opens a PR for the change.
# The weekly run often rebuilds a main commit an earlier run already handled, so it must not
# collide with that run's branch: it leaves an open PR for this image alone, or uses a new branch.
set -euo pipefail

: "${IMAGE_REPO:?}" "${NEW_IMAGE:?}" "${SHA_SHORT:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"
manifest=k8s/score-computation-cronjob.yaml
branch="chore/update-cronjob-image-$SHA_SHORT"

sed -i "s|image: $IMAGE_REPO:.*|image: $NEW_IMAGE|" "$manifest"

# sed succeeds even when nothing matches, so confirm the new tag was actually written
if ! grep -qF "image: $NEW_IMAGE" "$manifest"; then
  echo "::error::$manifest has no image under $IMAGE_REPO, so the new tag could not be written"
  exit 1
fi

if git diff --quiet -- "$manifest"; then
  echo "::notice::$manifest on this commit already uses $NEW_IMAGE, so no PR is needed"
  exit 0
fi

# Earlier runs for this commit pushed $branch, or $branch-run-<id>-<attempt> once it was taken.
open_prs=$(gh pr list --state open --limit 200 --json number,headRefName,isCrossRepository,url |
  jq -r --arg b "$branch" '.[]
    | select((.isCrossRepository | not) and (.headRefName == $b or (.headRefName | startswith($b + "-"))))
    | "\(.number) \(.headRefName) \(.url)"')

while read -r number head url; do
  [ -n "$number" ] || continue
  git fetch --quiet --depth=1 origin "refs/heads/$head"
  if proposed=$(git show "FETCH_HEAD:$manifest") && grep -qF "image: $NEW_IMAGE" <<< "$proposed"; then
    echo "::notice::PR #$number ($url) already moves the CronJob to $NEW_IMAGE, so it is left as is"
    exit 0
  fi
  echo "::warning::PR #$number from $head is open but does not use $NEW_IMAGE, so a new PR will be opened"
done <<< "$open_prs"

if [ -n "$(git ls-remote --heads origin "refs/heads/$branch")" ]; then
  run_branch="$branch-run-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
  echo "::notice::$branch already exists with no open PR for $NEW_IMAGE, so this run pushes $run_branch"
  branch="$run_branch"
fi

git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git checkout -b "$branch"
git add "$manifest"
git commit -m "chore(k8s): update cronjob image to sha-$SHA_SHORT"
git push -u origin "$branch"

gh pr create \
  --head "$branch" \
  --title "chore(k8s): update cronjob image to sha-$SHA_SHORT" \
  --body "Updates the CronJob to use image \`$NEW_IMAGE\`

**Merge order:** Land this **before** any open \`chore(main): release\` PR. After this lands, wait 1-2 minutes for release-please to force-push the release PR with the new SHA bump, otherwise you'll need to land a third PR.

After merging, run:
\`\`\`bash
kubectl apply -f k8s/score-computation-cronjob.yaml
\`\`\`"
