import re
from typing import Optional, Set

REGEX_ENDS_WITH_NUMBER = re.compile(r"(\D+)(\d+)$")


def match_icase(unknown_value: str, allowed_set: Set) -> Optional[str]:
    def try_case(value: str):
        return value.upper() == unknown_value.upper()

    match = list(filter(try_case, allowed_set))
    if len(match) > 0:
        return match[0]

    return None


def match_unknown_value(unknown_value: str, allowed_set: Set) -> Optional[str]:
    # Just try a case insensitive match
    match = match_icase(unknown_value, allowed_set)
    if match:
        return match

    # Try with a plural - good for EV
    match = match_icase(unknown_value + "s", allowed_set)
    if match:
        return match

    # Try with no "ation" good for Transportation
    if unknown_value.endswith("ation"):
        match = match_icase(unknown_value[0:-5], allowed_set)
        if match:
            return match

    # Try hyphenating trailing numbers - good for Covid19
    ends_with_number = REGEX_ENDS_WITH_NUMBER.match(unknown_value)

    if ends_with_number:
        hyphenated_number = (
            ends_with_number.groups()[0].strip() + "-" + ends_with_number.groups()[1]
        )

        match = match_icase(hyphenated_number, allowed_set)
        if match:
            return match

    # Try without an "es" ending
    if unknown_value.endswith("es"):
        no_plural = unknown_value[0:-2]

        match = match_icase(no_plural, allowed_set)
        if match:
            return match

    # Try stripping any spaces
    if " " in unknown_value:
        no_spaces = unknown_value.replace(" ", "")
        match = match_icase(no_spaces, allowed_set)
        if match:
            return match

    # Try hyphenating Co...
    if unknown_value.upper().startswith("CO"):
        hyphenated_co = "Co-" + unknown_value[2:].strip()
        match = match_icase(hyphenated_co, allowed_set)
        if match:
            return match

    # Try hyphenating multi
    if unknown_value.upper().startswith("MULTI "):
        hyphenated_multi = "Multi-" + unknown_value[5:].strip()
        match = match_icase(hyphenated_multi, allowed_set)
        if match:
            return match

    # Try adding brackets to multi words
    words = unknown_value.split(" ")
    if len(words) > 2:
        abbrev = "".join([w[0] for w in words])

        with_abbrev = f"{unknown_value} ({abbrev})"
        match = match_icase(with_abbrev, allowed_set)
        if match:
            return match

    return None
