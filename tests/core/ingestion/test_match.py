import pytest

from app.core.ingestion.match import match_unknown_value


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["this", "that", "value"]),
        set(["this", "value", "that"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["vAlue", "THIs"],
)
def test_match_unknown_value__ignores_case(input_value: str, allowed_set: set[str]):
    assert match_unknown_value(input_value, allowed_set)


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["cats", "dogs", "sheeps"]),
        set(["cows", "cats", "dogs"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["cAt", "DOG"],
)
def test_match_unknown_value__trys_plurals(input_value: str, allowed_set: set[str]):
    assert match_unknown_value(input_value, allowed_set)


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["Transport", "Cheese", "ill"]),
        set(["Ill", "Cheese", "transport"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["transportation", "illation"],
)
def test_match_unknown_value__removes_ation(input_value: str, allowed_set: set[str]):
    assert match_unknown_value(input_value, allowed_set)


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["covid-19", "20-things", "WD-40"]),
        set(["WD-40", "covid-19", "random"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["Covid19", "WD40"],
)
def test_match_unknown_value__hyphenates_number_at_end(
    input_value: str, allowed_set: set[str]
):
    assert match_unknown_value(input_value, allowed_set)


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["wax", "tax", "chicken"]),
        set(["tax", "wax", "chicken"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["taxes", "waxes"],
)
def test_match_unknown_value__removes_es(input_value: str, allowed_set: set[str]):
    assert match_unknown_value(input_value, allowed_set)


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["pingpong", "onetwo", "chicken"]),
        set(["chicken", "onetwo", "pingpong"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["ping pong", "one  two "],
)
def test_match_unknown_value__strip_space(input_value: str, allowed_set: set[str]):
    assert match_unknown_value(input_value, allowed_set)


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["co-habit", "co-operation", "cow"]),
        set(["co-operation", "cow", "co-habit"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["cooperation", "cohabit"],
)
def test_match_unknown_value__hyphenates_co(input_value: str, allowed_set: set[str]):
    assert match_unknown_value(input_value, allowed_set)


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["multi-modal", "multi-coloured", "cow"]),
        set(["cow", "multi-modal", "multi-coloured"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["multi modal", "multi coloured"],
)
def test_match_unknown_value__hyphenates_multi(input_value: str, allowed_set: set[str]):
    assert match_unknown_value(input_value, allowed_set)


@pytest.mark.parametrize(
    "allowed_set",
    [
        set(["one two three (ott)", "multi-coloured", "Climate policy Radar (CPR)"]),
        set(["cow", "Climate policy Radar (CPR)", "one two three (ott)"]),
    ],
)
@pytest.mark.parametrize(
    "input_value",
    ["one two Three", "Climate Policy Radar"],
)
def test_match_unknown_value__brackets_abbrev(input_value: str, allowed_set: set[str]):
    assert match_unknown_value(input_value, allowed_set)
