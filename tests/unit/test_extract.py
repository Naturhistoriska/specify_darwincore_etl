from typing import Any

from etl.extract import ArchiveMetadata, FieldMetadata, parse_meta_xml


def test_parse_meta_xml_simple(tmp_path: Any, mocker: Any) -> None:
    # Mock MetaDwCA
    mock_meta_dwca = mocker.patch("etl.extract.MetaDwCA", autospec=True)
    mock_instance = mock_meta_dwca.return_value

    # Simulate meta_elements from dwcahandler
    from dwcahandler.dwca import CoreOrExtType
    from dwcahandler.dwca.dwca_meta import Field, MetaElementAttributes, MetaElementInfo

    # Create mock field objects
    mock_fields = [
        Field(index="0", field_name="id", term="http://rs.tdwg.org/dwc/terms/occurrenceID"),
        Field(index="1", field_name="scientificName", term="http://rs.tdwg.org/dwc/terms/scientificName"),
    ]

    # Mock MetaElementInfo
    mock_info = mocker.Mock(spec=MetaElementInfo)
    mock_info.core_or_ext_type = CoreOrExtType.CORE
    mock_info.file_name = "occurrence.csv"
    mock_info.ignore_header_lines = "1"
    mock_info.type = mocker.Mock()
    mock_info.type.value = "http://rs.tdwg.org/dwc/terms/Occurrence"

    mock_csv_encoding = mocker.Mock()
    mock_csv_encoding.csv_delimiter = ","
    mock_csv_encoding.csv_text_enclosure = '"'
    mock_info.csv_encoding = mock_csv_encoding

    # Mock component
    mock_element = mocker.Mock(spec=MetaElementAttributes)
    mock_element.meta_element_type = mock_info
    mock_element.fields = mock_fields
    mock_element.core_id = mocker.Mock(spec=Field)
    mock_element.core_id.index = "0"

    mock_instance.meta_elements = [mock_element]

    # Create dummy meta.xml path
    meta_path = tmp_path / "meta.xml"
    meta_path.touch()

    result = parse_meta_xml(meta_path)

    assert isinstance(result, ArchiveMetadata)
    assert result.core.file_path == tmp_path / "occurrence.csv"
    assert len(result.core.fields) == 2
    assert result.core.fields[0].name == "occurrenceID"
    assert result.core.id_index == 0


def test_field_metadata_naming() -> None:
    field = FieldMetadata(index=0, term="http://rs.tdwg.org/dwc/terms/occurrenceID", default="defaultVal")
    assert field.name == "occurrenceID"

    # Test fallback name from URI
    field_uri = FieldMetadata(index=1, term="http://rs.tdwg.org/dwc/terms/eventDate")
    assert field_uri.name == "eventDate"
