#
# This is the core functionality taken out so it can be tested
#

clean_string() {
    echo "$1" | tr -d '\n' | tr -d ' '
}

is_tagged_version() {
    if [[ "$1" =~ refs/tags/v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)  ]]
    then
        return 0
    else
        return 1
    fi
}


get_major() {
    echo "${1}" | cut -d'.' -f1
}

get_minor() {
    echo "${1}" | cut -d'.' -f2
}

get_patch() {
    if [[ ${1} == *"-"* ]]; then
        echo "${1}" | cut -d'.' -f3 | cut -d'-' -f1
    else
        echo "${1}" | cut -d'.' -f3
    fi
}

get_maturity() {
    if [[ ${1} == *"-"* ]]; then
        echo "${1}" | cut -d'.' -f3 | cut -d'-' -f2
    else
        echo ""
    fi
}

get_docker_tags() {
    # Arguments:
    #  - Name reference for the array
    #  - Name of the docker image
    #  - The semver we are using to create the tags

    local -n arr=$1             # use nameref to create values
    name=$2
    semver=$3

    major=$(get_major "${semver}")
    minor=$(get_minor "${semver}")
    patch=$(get_patch "${semver}")
    maturity=$(get_maturity "${semver}")

    if [ -z ${maturity} ] ; then
        echo "Detected Version: ${major} . ${minor} . ${patch}"
        full_tag="${name}:${major}.${minor}.${patch}"
        minor_tag="${name}:${major}.${minor}"
        major_tag="${name}:${major}"
    else
        echo "Detected Version: ${major} . ${minor} . ${patch} [${maturity}]"
        full_tag="${name}:${major}.${minor}.${patch}-${maturity}"
        minor_tag="${name}:${major}.${minor}-${maturity}"
        major_tag="${name}:${major}-${maturity}"
    fi
    arr=($full_tag $minor_tag $major_tag)
}
