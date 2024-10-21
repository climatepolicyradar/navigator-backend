from app.repository.pipeline import _flatten_pipeline_metadata


def test_flatten_pipeline_metadata():
    family_metadata = {"a": ["1"], "b": ["2"]}
    doc_metadata = {"a": ["3"], "b": ["4"]}
    result = _flatten_pipeline_metadata(family_metadata, doc_metadata)

    assert len(result) == 4
    assert result["family.a"] == ["1"]
    assert result["family.b"] == ["2"]
    assert result["document.a"] == ["3"]
    assert result["document.b"] == ["4"]
