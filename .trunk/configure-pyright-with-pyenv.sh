#! /usr/bin/env bash
set -e

# Get the name of the expected venv for this repo from the pyproject.toml file.
venv_name=$(grep -m 1 venv pyproject.toml | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3) || true

# Check if pyrightconfig already exists.
if [[ ! -f pyrightconfig.json ]]; then
	# Check if pyenv-pyright plugin is installed
	if ! command -v pyenv &>/dev/null; then
		echo "pyenv not installed. Please install pyenv..."
		exit 1
	fi

	pyenv_root=$(pyenv root)
	dir_path="${pyenv_root}"/plugins/pyenv-pyright
	if [[ ! -d ${dir_path} ]]; then
		# trunk-ignore(shellcheck/SC2312)
		if [[ -n $(ls -A "${dir_path}") ]]; then
			git clone https://github.com/alefpereira/pyenv-pyright.git "${dir_path}"
		fi
	fi

	# Generate the pyrightconfig.json file.
	pyenv pyright "${venv_name}"
	pyenv local "${venv_name}"
fi

# Check whether required keys are present in pyrightconfig.json.
if ! jq -r --arg venv_name "${venv_name}" '. | select(.venv != $venv_name) | select(.venvPath != null)' pyrightconfig.json; then
	echo "Failed to configure pyright to use pyenv environment '${venv_name}' as interpreter. Please check pyrightconfig.json..."
	exit 1
fi
exit 0
