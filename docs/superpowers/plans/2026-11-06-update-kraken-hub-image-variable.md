# Update KRAKEN_HUB_IMAGE Group Variable Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically update the GitLab group-level CI/CD variable `KRAKEN_HUB_IMAGE` to the newly published image tag whenever a new version of `kraken-hub` is released on `main`.

**Architecture:** In `kraken-hub/.gitlab-ci.yml`, within the `release` job script, immediately after the Docker image is built and pushed to Harbor, invoke the GitLab REST API (`PUT /groups/:id/variables/KRAKEN_HUB_IMAGE` with fallback to `POST`) using the group token `$GITLAB_GROUP_TOKEN`.

**Tech Stack:** GitLab CI/CD, GitLab REST API v4, `curl`, `sh`/`bash`.

**Spec:** User instruction to trigger an automatic step updating `KRAKEN_HUB_IMAGE` to the new version on every release of `kraken-hub`.

## Global Constraints

- Must run only on `main` branch during the release stage after successful image push to Harbor.
- Must use `$GITLAB_GROUP_TOKEN` for authentication against the GitLab API.
- Must target `$CI_API_V4_URL/groups/$CI_PROJECT_ROOT_NAMESPACE/variables/KRAKEN_HUB_IMAGE`.
- Must handle both existing variable (`PUT`) and new variable (`POST` fallback if 404).
- Must fail the release job if `GITLAB_GROUP_TOKEN` is present but the API update fails, preventing silent configuration drift.

## Review Focus

- **Missing `GITLAB_GROUP_TOKEN`:** If the variable is unset or empty, the script should fail with a clear diagnostic message indicating the required token is missing.
- **Variable already exists:** A `PUT` request updates the `value` field and returns HTTP 200.
- **Variable does not exist yet:** A `PUT` returns HTTP 404, triggering a `POST` with `key=KRAKEN_HUB_IMAGE` and `value`, returning HTTP 201.
- **API Error / Invalid Token:** Any non-200/201 response code prints the HTTP status code and fails the pipeline step (`exit 1`).
- **Idempotency:** Re-running the release job or building the same tag must safely update or overwrite the variable with the same value.

---

### Task 1: Add automated `KRAKEN_HUB_IMAGE` update to `kraken-hub` release pipeline

**Files:**
- Modify: `kraken-hub/.gitlab-ci.yml`

**Interfaces:**
- Consumes: `$HARBOR_HOST`, `$HARBOR_PROJECT`, `$CI_PROJECT_NAME`, `$VERSION`, `$GITLAB_GROUP_TOKEN`, `$CI_API_V4_URL`, `$CI_PROJECT_ROOT_NAMESPACE`
- Produces: Updated `KRAKEN_HUB_IMAGE` group variable in GitLab (e.g., `harbor.damoreira.ml/kraken-org/kraken-hub:0.2.1`)

- [ ] **Step 1: Inspect `kraken-hub/.gitlab-ci.yml` release script structure**

Review lines 120-140 of `kraken-hub/.gitlab-ci.yml` where Docker images are pushed to Harbor.

- [ ] **Step 2: Add group variable update logic to `kraken-hub/.gitlab-ci.yml`**

In `kraken-hub/.gitlab-ci.yml`, directly after `docker push "$IMAGE:latest"`:
```yaml
      NEW_HUB_IMAGE="$IMAGE:$VERSION"
      echo "Updating GitLab group variable KRAKEN_HUB_IMAGE to $NEW_HUB_IMAGE..."
      if [ -z "${GITLAB_GROUP_TOKEN:-}" ]; then
        echo "ERROR: GITLAB_GROUP_TOKEN is not set. Cannot update KRAKEN_HUB_IMAGE."
        exit 1
      fi
      STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
        --request PUT \
        --header "PRIVATE-TOKEN: $GITLAB_GROUP_TOKEN" \
        --form "value=$NEW_HUB_IMAGE" \
        "$CI_API_V4_URL/groups/$CI_PROJECT_ROOT_NAMESPACE/variables/KRAKEN_HUB_IMAGE")
      if [ "$STATUS" = "404" ]; then
        echo "Variable KRAKEN_HUB_IMAGE not found, creating it via POST..."
        STATUS=$(curl --silent --output /dev/null --write-out '%{http_code}' \
          --request POST \
          --header "PRIVATE-TOKEN: $GITLAB_GROUP_TOKEN" \
          --form "key=KRAKEN_HUB_IMAGE" \
          --form "value=$NEW_HUB_IMAGE" \
          "$CI_API_V4_URL/groups/$CI_PROJECT_ROOT_NAMESPACE/variables")
      fi
      case "$STATUS" in
        200|201) echo "Successfully updated group variable KRAKEN_HUB_IMAGE to $NEW_HUB_IMAGE." ;;
        *) echo "Failed to update group variable KRAKEN_HUB_IMAGE (HTTP $STATUS)"; exit 1 ;;
      esac
```

- [ ] **Step 3: Validate syntax and lint `.gitlab-ci.yml`**

Verify YAML validity with python YAML parser.

- [ ] **Step 4: Commit changes to a feature branch**

```bash
git checkout -b ci/auto-update-hub-image
git add .gitlab-ci.yml
git commit -m "ci: update KRAKEN_HUB_IMAGE group variable on release"
```

- [ ] **Step 5: Push branch, create merge request, and merge to `main`**

Push branch, open MR via GitLab API, await pipeline success, and merge into `main` to trigger release job.
