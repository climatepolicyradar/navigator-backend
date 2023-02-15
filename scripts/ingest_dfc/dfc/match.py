import re
from typing import Optional, Set

REGEX_ENDS_WITH_NUMBER = re.compile(r'(\D+)(\d+)$')

def icmp(a: str,b: str) -> bool: return a.upper() == b.upper()

def match_icase(unknown_value: str, allowed_set: Set) -> Optional[str]:
    def try_case(value: str):
        return icmp(value, unknown_value)

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
    match = match_icase(unknown_value+"s", allowed_set)
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
        hyphenated_number = ends_with_number.groups()[0].strip() + "-" + ends_with_number.groups()[1]

        match = match_icase(hyphenated_number, allowed_set)
        if match:
            return match

    if unknown_value.endswith("es"):
        no_plural = unknown_value[0:-2]

        match = match_icase(no_plural, allowed_set)
        if match:
            return match

    return None

