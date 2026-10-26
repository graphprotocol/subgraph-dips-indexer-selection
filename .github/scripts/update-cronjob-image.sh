#!/usr/bin/env bash
# Points every manifest that runs the score computation image at a new build and opens a PR.
# The weekly run often rebuilds a main commit an earlier run already handled, so it must not
# collide with that run's branch: it leaves an open PR for this image alone, or uses a new branch.
set -euo pipefail

: "${IMAGE_REPO:?}" "${NEW_IMAGE:?}" "${SHA_SHORT:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"
branch="chore/update-cronjob-image-$SHA_SHORT"

# Every manifest that runs this image moves together, so a new one is picked up without editing
# this script.
manifests=()
while IFS= read -r manifest; do
  manifests+=("$manifest")
done < <(grep -rlF "image: $IMAGE_REPO:" k8s/)
if [ "${#manifests[@]}" -eq 0 ]; then
  echo "::error::no manifest under k8s/ uses $IMAGE_REPO, so the new tag could not be written"
  exit 1
fi

for manifest in "${manifests[@]}"; do
  sed -i "s|image: $IMAGE_REPO:.*|image: $NEW_IMAGE|" "$manifest"

  # sed succeeds even when nothing matches, so confirm the new tag was actually written
  if ! grep -qF "image: $NEW_IMAGE" "$manifest"; then
    echo "::error::$manifest has no image under $IMAGE_REPO, so the new tag could not be written"
    exit 1
  fi
done

if git diff --quiet -- "${manifests[@]}"; then
  echo "::notice::every manifest on this commit already uses $NEW_IMAGE, so no PR is needed"
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
  # An open PR is enough only if it moves every manifest to this image.
  covered=true
  for manifest in "${manifests[@]}"; do
    if ! proposed=$(git show "FETCH_HEAD:$manifest") || ! grep -qF "image: $NEW_IMAGE" <<< "$proposed"; then
      covered=false
      break
    fi
  done
  if [ "$covered" = true ]; then
    echo "::notice::PR #$number ($url) already moves every manifest to $NEW_IMAGE, so it is left as is"
    exit 0
  fi
  echo "::warning::PR #$number from $head is open but does not move every manifest to $NEW_IMAGE, so a new PR will be opened"
done <<< "$open_prs"

if [ -n "$(git ls-remote --heads origin "refs/heads/$branch")" ]; then
  run_branch="$branch-run-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
  echo "::notice::$branch already exists with no open PR for $NEW_IMAGE, so this run pushes $run_branch"
  branch="$run_branch"
fi

git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git checkout -b "$branch"
git add "${manifests[@]}"
git commit -m "chore(k8s): update cronjob image to sha-$SHA_SHORT" \
  -m "Updates every manifest that runs this image: ${manifests[*]}"
git push -u origin "$branch"

gh pr create \
  --head "$branch" \
  --title "chore(k8s): update cronjob image to sha-$SHA_SHORT" \
  --body "Updates the CronJob and the sync-status fetcher to use image \`$NEW_IMAGE\`

**Merge order:** Land this **before** any open \`chore(main): release\` PR. After this lands, wait 1-2 minutes for release-please to force-push the release PR with the new SHA bump, otherwise you'll need to land a third PR.

After merging, run:
\`\`\`bash
kubectl apply $(printf -- '-f %s ' "${manifests[@]}")
\`\`\`"
