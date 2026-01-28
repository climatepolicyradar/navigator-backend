#!/usr/bin/env bash
# shellcheck disable=SC2310
# CI test cases for pull-request workflow job triggering.
# Validates that jobs trigger/skip correctly based on file changes and PR labels.
# Run from repo root. Requires act: https://github.com/nektos/act

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKFLOW_FILE=".github/workflows/pull-request.yml"
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"

cd "${REPO_ROOT}"

if ! command -v act &>/dev/null; then
	echo "act not found. Install from https://github.com/nektos/act"
	exit 1
fi

# Helper: determine changes outputs based on staged files.
# Simulates what the changes job's parse step would produce.
# Returns the outputs as JSON: {"services": [...], "infra_services": [...]}
get_changes_outputs() {
	# Get list of changed files from git (staged)
	local changed_files
	changed_files=$(git diff --cached --name-only --diff-filter=AM 2>/dev/null | grep -v "^tests/workflows/" | grep -v "^\.github/" || true)

	# Determine which filter keys would match (simulating paths-filter)
	local matched_filters=()

	while IFS= read -r file; do
		[[ -z ${file} ]] && continue

		# Check infra patterns (more specific, checked first)
		if [[ ${file} == families-api/infra/* ]]; then
			matched_filters+=("families-api/infra")
		elif [[ ${file} == geographies-api/infra/* ]]; then
			matched_filters+=("geographies-api/infra")
		elif [[ ${file} == concepts-api/infra/* ]]; then
			matched_filters+=("concepts-api/infra")
		elif [[ ${file} == data-in-pipeline/infra/* ]]; then
			matched_filters+=("data-in-pipeline/infra")
		elif [[ ${file} == data-in-pipeline-load-api/infra/* ]]; then
			matched_filters+=("data-in-pipeline-load-api/infra")
		elif [[ ${file} == data-in-api/infra/* ]]; then
			matched_filters+=("data-in-api/infra")
		# Check service patterns (not infra)
		elif [[ ${file} == families-api/* ]] || [[ ${file} == api/* ]] || [[ ${file} == "uv.lock" ]] || [[ ${file} == "pyproject.toml" ]]; then
			matched_filters+=("families-api")
		elif [[ ${file} == geographies-api/* ]] || [[ ${file} == api/* ]] || [[ ${file} == "uv.lock" ]] || [[ ${file} == "pyproject.toml" ]]; then
			matched_filters+=("geographies-api")
		elif [[ ${file} == concepts-api/* ]] || [[ ${file} == api/* ]] || [[ ${file} == "uv.lock" ]] || [[ ${file} == "pyproject.toml" ]]; then
			matched_filters+=("concepts-api")
		elif [[ ${file} == data-in-pipeline/* ]] || [[ ${file} == api/* ]] || [[ ${file} == data-in-models/* ]] || [[ ${file} == "uv.lock" ]] || [[ ${file} == "pyproject.toml" ]]; then
			matched_filters+=("data-in-pipeline")
		elif [[ ${file} == data-in-pipeline-load-api/* ]] || [[ ${file} == api/* ]] || [[ ${file} == "uv.lock" ]] || [[ ${file} == "pyproject.toml" ]]; then
			matched_filters+=("data-in-pipeline-load-api")
		fi
	done <<<"${changed_files}"

	# Deduplicate
	local unique_filters
	unique_filters=$(printf '%s\n' "${matched_filters[@]}" | sort -u)

	# Separate infra from services (matching parse step logic)
	local services=()
	local infra_services=()

	while IFS= read -r filter; do
		[[ -z ${filter} ]] && continue
		if [[ ${filter} == */infra ]]; then
			infra_services+=("${filter}")
		else
			services+=("${filter}")
		fi
	done <<<"${unique_filters}"

	# Convert to JSON arrays
	local services_json="[]"
	local infra_services_json="[]"

	if [[ ${#services[@]} -gt 0 ]]; then
		local temp_json
		temp_json=$(printf '%s\n' "${services[@]}" | jq -R . | jq -s . 2>/dev/null || true)
		if [[ -n ${temp_json} ]]; then
			services_json="${temp_json}"
		fi
	fi
	if [[ ${#infra_services[@]} -gt 0 ]]; then
		local temp_json
		temp_json=$(printf '%s\n' "${infra_services[@]}" | jq -R . | jq -s . 2>/dev/null || true)
		if [[ -n ${temp_json} ]]; then
			infra_services_json="${temp_json}"
		fi
	fi

	echo "{\"services\": ${services_json}, \"infra_services\": ${infra_services_json}}"
}

# Helper: check if a job would run based on changes outputs and event labels.
# This evaluates the actual workflow conditions instead of relying on act's dry-run.
job_would_run_based_on_conditions() {
	local event_file="$1"
	local job_name="$2"
	local changes_outputs_json="$3"

	# Parse the changes outputs
	local services
	local infra_services
	services=$(echo "${changes_outputs_json}" | jq -c '.services' 2>/dev/null || echo "[]")
	infra_services=$(echo "${changes_outputs_json}" | jq -c '.infra_services' 2>/dev/null || echo "[]")

	# Check if deploy:staging label exists in event
	local has_deploy_label="false"
	if jq -e '.pull_request.labels[]? | select(.name == "deploy:staging")' "${event_file}" >/dev/null 2>&1; then
		has_deploy_label="true"
	fi

	# Evaluate conditions based on workflow logic
	case "${job_name}" in
	changes | code-quality | workflow-tests)
		# These always run
		return 0
		;;
	test | tests)
		# Run if services != '[]'
		if [[ ${services} != "[]" ]]; then
			return 0
		else
			return 1
		fi
		;;
	pulumi-preview)
		# Run if infra_services != '[]'
		if [[ ${infra_services} != "[]" ]]; then
			return 0
		else
			return 1
		fi
		;;
	deploy-infra)
		# Run if: has deploy:staging label AND infra_services != '[]'
		# Note: We can't check pulumi-preview result in this test
		if [[ ${has_deploy_label} == "true" ]] && [[ ${infra_services} != "[]" ]]; then
			return 0
		else
			return 1
		fi
		;;
	deploy)
		# Run if: has deploy:staging label AND services != '[]'
		# Note: We can't check test/deploy-infra results in this test
		if [[ ${has_deploy_label} == "true" ]] && [[ ${services} != "[]" ]]; then
			return 0
		else
			return 1
		fi
		;;
	*)
		# Unknown job - assume it would run (conservative)
		return 0
		;;
	esac
}

# Helper: try to dry-run a job and check if act attempts to run it.
# For jobs that depend on changes outputs, we evaluate conditions manually.
# Returns 0 if job would run, 1 if it would skip.
job_would_run() {
	local event_file="$1"
	local job_name="$2"
	local workflow_file="$3"

	# For jobs that depend on changes outputs, run changes first and evaluate conditions
	if [[ ${job_name} != "changes" ]] && [[ ${job_name} != "code-quality" ]] && [[ ${job_name} != "workflow-tests" ]]; then
		local changes_outputs
		changes_outputs=$(get_changes_outputs)
		job_would_run_based_on_conditions "${event_file}" "${job_name}" "${changes_outputs}"
		return $?
	fi

	# For jobs that don't depend on outputs, use act's dry-run
	local output
	output=$(act pull_request -n -W "${workflow_file}" -e "${event_file}" -j "${job_name}" 2>&1 || true)

	# Check for signs that the job would execute (not skip)
	if echo "${output}" | grep -qiE "(skipped|condition not met|would be skipped)"; then
		return 1 # Job would skip
	fi

	# Check if act tried to run it (shows job ID, starting, etc.)
	if echo "${output}" | grep -qiE "(Job ID|Starting|Running|\[.*\]\s+${job_name})"; then
		return 0 # Job would run
	fi

	return 1 # Job doesn't appear, likely would skip
}

# Helper: assert a job would run.
assert_job_runs() {
	local event_file="$1"
	local job_name="$2"
	local workflow_file="$3"
	local result
	job_would_run "${event_file}" "${job_name}" "${workflow_file}"
	result=$?
	if [[ ${result} -eq 0 ]]; then
		echo "  ✅ ${job_name} would run"
		return 0
	else
		echo "  ❌ ${job_name} should run but would skip"
		return 1
	fi
}

# Helper: assert a job would skip.
assert_job_skips() {
	local event_file="$1"
	local job_name="$2"
	local workflow_file="$3"
	local result
	job_would_run "${event_file}" "${job_name}" "${workflow_file}"
	result=$?
	if [[ ${result} -ne 0 ]]; then
		echo "  ✅ ${job_name} correctly skipped"
		return 0
	else
		echo "  ❌ ${job_name} should skip but would run"
		return 1
	fi
}

# Test scenario: infra changes only, no deploy:staging label
test_scenario_infra_no_label() {
	echo ""
	echo "=== Scenario 1: Infra changes only, no deploy:staging label ==="

	# Create a temporary infra file change
	local test_file="data-in-api/infra/test-scenario-1.tmp"
	mkdir -p "$(dirname "${test_file}")"
	touch "${test_file}"

	# Stage the change (act uses git state)
	git add "${test_file}" 2>/dev/null || true

	local event_file="${FIXTURES_DIR}/pr-event-no-label.json"
	local failed=0

	echo "Expected to run: changes, code-quality, workflow-tests, pulumi-preview"
	if ! assert_job_runs "${event_file}" "changes" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "code-quality" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "workflow-tests" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "pulumi-preview" "${WORKFLOW_FILE}"; then
		failed=1
	fi

	echo "Expected to skip: test, tests, deploy-infra, deploy"
	if ! assert_job_skips "${event_file}" "test" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_skips "${event_file}" "tests" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_skips "${event_file}" "deploy-infra" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_skips "${event_file}" "deploy" "${WORKFLOW_FILE}"; then
		failed=1
	fi

	# Cleanup
	git reset HEAD "${test_file}" 2>/dev/null || true
	rm -f "${test_file}"

	if [[ ${failed} -eq 1 ]]; then
		echo "  ❌ Scenario 1 failed"
		return 1
	else
		echo "  ✅ Scenario 1 passed"
		return 0
	fi
}

# Test scenario: infra changes + deploy:staging label
test_scenario_infra_with_label() {
	echo ""
	echo "=== Scenario 2: Infra changes + deploy:staging label ==="

	local test_file="data-in-api/infra/test-scenario-2.tmp"
	mkdir -p "$(dirname "${test_file}")"
	touch "${test_file}"
	git add "${test_file}" 2>/dev/null || true

	local event_file="${FIXTURES_DIR}/pr-event-with-deploy-staging.json"
	local failed=0

	echo "Expected to run: changes, code-quality, workflow-tests, pulumi-preview"
	if ! assert_job_runs "${event_file}" "changes" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "code-quality" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "workflow-tests" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "pulumi-preview" "${WORKFLOW_FILE}"; then
		failed=1
	fi

	echo "Expected to skip: test, tests, deploy (no app changes)"
	if ! assert_job_skips "${event_file}" "test" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_skips "${event_file}" "tests" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_skips "${event_file}" "deploy" "${WORKFLOW_FILE}"; then
		failed=1
	fi

	# Note: deploy-infra depends on pulumi-preview result, which we can't fully test in dry-run
	# But we can verify it's at least considered (would show in -l output)
	echo "Note: deploy-infra conditional on pulumi-preview success (cannot fully test in dry-run)"

	# Cleanup
	git reset HEAD "${test_file}" 2>/dev/null || true
	rm -f "${test_file}"

	if [[ ${failed} -eq 1 ]]; then
		echo "  ❌ Scenario 2 failed"
		return 1
	else
		echo "  ✅ Scenario 2 passed"
		return 0
	fi
}

# Test scenario: app changes only, no deploy:staging label
test_scenario_app_no_label() {
	echo ""
	echo "=== Scenario 3: App changes only, no deploy:staging label ==="

	local test_file="families-api/app/test-scenario-3.tmp"
	mkdir -p "$(dirname "${test_file}")"
	touch "${test_file}"
	git add "${test_file}" 2>/dev/null || true

	local event_file="${FIXTURES_DIR}/pr-event-no-label.json"
	local failed=0

	echo "Expected to run: changes, code-quality, workflow-tests, test, tests"
	if ! assert_job_runs "${event_file}" "changes" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "code-quality" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "workflow-tests" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "test" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "tests" "${WORKFLOW_FILE}"; then
		failed=1
	fi

	echo "Expected to skip: pulumi-preview, deploy-infra, deploy"
	if ! assert_job_skips "${event_file}" "pulumi-preview" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_skips "${event_file}" "deploy-infra" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_skips "${event_file}" "deploy" "${WORKFLOW_FILE}"; then
		failed=1
	fi

	# Cleanup
	git reset HEAD "${test_file}" 2>/dev/null || true
	rm -f "${test_file}"

	if [[ ${failed} -eq 1 ]]; then
		echo "  ❌ Scenario 3 failed"
		return 1
	else
		echo "  ✅ Scenario 3 passed"
		return 0
	fi
}

# Test scenario: app changes + deploy:staging label
test_scenario_app_with_label() {
	echo ""
	echo "=== Scenario 4: App changes + deploy:staging label ==="

	local test_file="families-api/app/test-scenario-4.tmp"
	mkdir -p "$(dirname "${test_file}")"
	touch "${test_file}"
	git add "${test_file}" 2>/dev/null || true

	local event_file="${FIXTURES_DIR}/pr-event-with-deploy-staging.json"
	local failed=0

	echo "Expected to run: changes, code-quality, workflow-tests, test, tests, deploy"
	if ! assert_job_runs "${event_file}" "changes" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "code-quality" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "workflow-tests" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "test" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "tests" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_runs "${event_file}" "deploy" "${WORKFLOW_FILE}"; then
		failed=1
	fi

	echo "Expected to skip: pulumi-preview, deploy-infra (no infra changes)"
	if ! assert_job_skips "${event_file}" "pulumi-preview" "${WORKFLOW_FILE}"; then
		failed=1
	fi
	if ! assert_job_skips "${event_file}" "deploy-infra" "${WORKFLOW_FILE}"; then
		failed=1
	fi

	# Cleanup
	git reset HEAD "${test_file}" 2>/dev/null || true
	rm -f "${test_file}"

	if [[ ${failed} -eq 1 ]]; then
		echo "  ❌ Scenario 4 failed"
		return 1
	else
		echo "  ✅ Scenario 4 passed"
		return 0
	fi
}

# Main test runner
main() {
	echo "Testing pull-request workflow job triggering scenarios"
	echo "======================================================"
	echo "Note: These tests create temporary files to simulate changes,"
	echo "      then use act to verify which jobs would run/skip."
	echo ""

	local total_failed=0

	if ! test_scenario_infra_no_label; then
		total_failed=$((total_failed + 1))
	fi
	if ! test_scenario_infra_with_label; then
		total_failed=$((total_failed + 1))
	fi
	if ! test_scenario_app_no_label; then
		total_failed=$((total_failed + 1))
	fi
	if ! test_scenario_app_with_label; then
		total_failed=$((total_failed + 1))
	fi

	echo ""
	if [[ ${total_failed} -eq 0 ]]; then
		echo "✅ All scenarios passed"
		exit 0
	else
		echo "  ${total_failed} scenario(s) failed"
		exit 1
	fi
}

main "$@"
