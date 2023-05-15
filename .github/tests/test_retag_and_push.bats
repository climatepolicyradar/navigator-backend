#!/usr/bin/env bats
load '/opt/bats-test-helpers/bats-support/load.bash'
load '/opt/bats-test-helpers/bats-assert/load.bash'
load '/opt/bats-test-helpers/lox-bats-mock/stub.bash'

# ------

@test "clean_string removes CR" {
  source /code/funcs.sh
  str=$(printf " test \n ")
  run clean_string $str
  [ "$status" -eq 0 ]
  assert_output "test"
}

@test "clean_string removes all white spaces" {
  source /code/funcs.sh
  run clean_string "  test  "
  [ "$status" -eq 0 ]
  assert_output "test"
}

# ------

@test "is_tagged_version succeeds for typical version" {
  source /code/funcs.sh
  run is_tagged_version "refs/tags/v0.1.2-alpha"
  [ "$status" -eq 0 ]
}

@test "is_tagged_version fails for missing patch " {
  source /code/funcs.sh
  run is_tagged_version "refs/tags/v0.1-alpha"
  [ "$status" -eq 1 ]
}

@test "is_tagged_version fails for other tags " {
  source /code/funcs.sh
  run is_tagged_version "refs/tags/vimto"
  [ "$status" -eq 1 ]
}

# ------

@test "get_major returns major version" {
  source /code/funcs.sh
  run get_major "8.9.7-alpha"
  [ "$status" -eq 0 ]
  [ "$output" == "8" ]
}

# ------

@test "get_minor returns minor version" {
  source /code/funcs.sh
  run get_minor "8.9.7-alpha"
  [ "$status" -eq 0 ]
  [ "$output" == "9" ]
}

# ------

@test "get_patch returns patch version" {
  source /code/funcs.sh
  run get_patch "8.9.7-alpha"
  [ "$status" -eq 0 ]
  [ "$output" == "7" ]
}

# ------

@test "get_maturity returns maturity version" {
  source /code/funcs.sh
  run get_maturity "8.9.7-alpha"
  [ "$status" -eq 0 ]
  [ "$output" == "alpha" ]
}

# ------

@test "get_maturity returns empty " {
  source /code/funcs.sh
  run get_maturity "8.9.7"
  [ "$status" -eq 0 ]
  [ "$output" == "" ]
}

# ------

@test "regression with missing maturity" {
  source /code/funcs.sh
  semver="8.9.7"

  run get_major $semver
  [ "$status" -eq 0 ]
  [ "$output" == "8" ]
  run get_minor $semver
  [ "$status" -eq 0 ]
  [ "$output" == "9" ]
  run get_patch $semver
  [ "$status" -eq 0 ]
  [ "$output" == "7" ]
  run get_maturity $semver
  [ "$status" -eq 0 ]
  [ "$output" == "" ]
}

# ------

@test "get_docker_tags returns correctly with maturity " {
  source /code/funcs.sh
  run get_docker_tags tag_array "test" "8.9.7-alpha"
  [ "$status" -eq 0 ]

  # Note the above "run" command does something unholy - so just execute without it
  local tag_array
  get_docker_tags tag_array "test" "8.9.7-alpha"

  # Test we have 3 tags
  [ ${#tag_array[@]} -eq 3 ]

  [ "${tag_array[0]}" == "test:8.9.7-alpha" ]
  [ "${tag_array[1]}" == "test:8.9-alpha" ]
  [ "${tag_array[2]}" == "test:8-alpha" ]
}

# ------

@test "get_docker_tags returns correctly without maturity " {
  source /code/funcs.sh
  run get_docker_tags tag_array "test" "8.9.7"
  [ "$status" -eq 0 ]

  # Note the above "run" command does something unholy - so just execute without it
  local tag_array
  get_docker_tags tag_array "test" "8.9.7"

  # Test we have 3 tags
  [ ${#tag_array[@]} -eq 3 ]

  [ "${tag_array[0]}" == "test:8.9.7" ]
  [ "${tag_array[1]}" == "test:8.9" ]
  [ "${tag_array[2]}" == "test:8" ]
}

