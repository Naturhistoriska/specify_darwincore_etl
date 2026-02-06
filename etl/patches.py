import logging
from typing import Any

from dwcahandler.dwca import MetaDwCA


def apply_dwcahandler_patches() -> None:
    """Applies runtime patches to dwcahandler to improve robustness."""
    _original_extract_meta_info = MetaDwCA._MetaDwCA__extract_meta_info

    def _patched_extract_meta_info(self: Any, ns: str, node_elm: Any, core_or_ext_type: Any) -> Any:
        # Set default values for optional attributes if they are missing in the XML
        defaults = {
            "ignoreHeaderLines": "0",
            "encoding": "UTF-8",
            "fieldsEnclosedBy": '"',
            "fieldsTerminatedBy": ",",
            "linesTerminatedBy": "\n",
        }
        for attr, default in defaults.items():
            if attr not in node_elm.attrib:
                node_elm.attrib[attr] = default
        return _original_extract_meta_info(self, ns, node_elm, core_or_ext_type)

    MetaDwCA._MetaDwCA__extract_meta_info = _patched_extract_meta_info
    logging.debug("Applied dwcahandler monkey-patches.")
