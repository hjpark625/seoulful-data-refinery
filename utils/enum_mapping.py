import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(root_dir))

from enums.category import CategorySeq, CategoryLabel  # noqa: E402
from enums.gu import GuSeq, GuLabel  # noqa: E402


category_label_to_seq = {
    label.value: seq.value for label, seq in zip(CategoryLabel, CategorySeq)
}
gu_label_to_seq = {label.value: seq.value for label, seq in zip(GuLabel, GuSeq)}


def get_enum_seq(label: GuLabel, enum_dict: dict[str, int]):
    return enum_dict.get(label, CategorySeq.OTHER.value)
