"""Qt court picker for CourtListener searches."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
)

from court_catalog import CATALOG


class CourtPickerDialog(QDialog):
    """Grouped checkable tree of CourtListener court IDs."""

    def __init__(self, selected: set[str], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Select Courts")
        self.resize(520, 660)
        self._updating = False
        self._item_ids: dict[QTreeWidgetItem, str] = {}

        layout = QVBoxLayout(self)
        self.summary = QLabel()
        layout.addWidget(self.summary)

        self.tree = QTreeWidget()
        self.tree.setHeaderHidden(True)
        layout.addWidget(self.tree, 1)

        self._build_nodes(None, CATALOG, selected)
        for i in range(self.tree.topLevelItemCount()):
            self.tree.topLevelItem(i).setExpanded(True)

        self.tree.itemChanged.connect(self._on_item_changed)
        self._refresh_summary()

        action_row = QHBoxLayout()
        clear_btn = QPushButton("All Courts")
        clear_btn.clicked.connect(self.clear_selection)
        action_row.addWidget(clear_btn)
        action_row.addStretch(1)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        action_row.addWidget(buttons)
        layout.addLayout(action_row)

    def selected_courts(self) -> set[str]:
        return {
            court_id
            for item, court_id in self._item_ids.items()
            if item.checkState(0) == Qt.CheckState.Checked
        }

    def clear_selection(self) -> None:
        self._updating = True
        try:
            for item in self._all_items():
                item.setCheckState(0, Qt.CheckState.Unchecked)
        finally:
            self._updating = False
        self._refresh_summary()

    def _build_nodes(self, parent: QTreeWidgetItem | None, nodes, selected: set[str]) -> None:
        for label_or_id, payload in nodes:
            if isinstance(payload, list):
                item = QTreeWidgetItem([str(label_or_id)])
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                item.setCheckState(0, Qt.CheckState.Unchecked)
                self._add_child(parent, item)
                self._build_nodes(item, payload, selected)
                self._sync_group_state(item)
            else:
                court_id = str(label_or_id)
                item = QTreeWidgetItem([f"{payload}"])
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                item.setData(0, Qt.ItemDataRole.UserRole, court_id)
                item.setToolTip(0, court_id)
                item.setCheckState(
                    0,
                    Qt.CheckState.Checked if court_id in selected else Qt.CheckState.Unchecked,
                )
                self._item_ids[item] = court_id
                self._add_child(parent, item)

    def _add_child(self, parent: QTreeWidgetItem | None, item: QTreeWidgetItem) -> None:
        if parent is None:
            self.tree.addTopLevelItem(item)
        else:
            parent.addChild(item)

    def _on_item_changed(self, item: QTreeWidgetItem, _column: int) -> None:
        if self._updating:
            return
        self._updating = True
        try:
            state = item.checkState(0)
            if item.childCount():
                self._set_descendants(item, state)
            self._sync_ancestors(item.parent())
        finally:
            self._updating = False
        self._refresh_summary()

    def _set_descendants(self, item: QTreeWidgetItem, state: Qt.CheckState) -> None:
        for i in range(item.childCount()):
            child = item.child(i)
            child.setCheckState(0, state)
            self._set_descendants(child, state)

    def _sync_ancestors(self, item: QTreeWidgetItem | None) -> None:
        while item is not None:
            self._sync_group_state(item)
            item = item.parent()

    def _sync_group_state(self, item: QTreeWidgetItem) -> None:
        if not item.childCount():
            return
        states = {item.child(i).checkState(0) for i in range(item.childCount())}
        if states == {Qt.CheckState.Checked}:
            item.setCheckState(0, Qt.CheckState.Checked)
        elif states == {Qt.CheckState.Unchecked}:
            item.setCheckState(0, Qt.CheckState.Unchecked)
        else:
            item.setCheckState(0, Qt.CheckState.PartiallyChecked)

    def _all_items(self) -> list[QTreeWidgetItem]:
        out: list[QTreeWidgetItem] = []

        def walk(item: QTreeWidgetItem) -> None:
            out.append(item)
            for i in range(item.childCount()):
                walk(item.child(i))

        for i in range(self.tree.topLevelItemCount()):
            walk(self.tree.topLevelItem(i))
        return out

    def _refresh_summary(self) -> None:
        count = len(self.selected_courts())
        self.summary.setText(
            "All courts are included." if count == 0 else f"{count} court(s) selected."
        )


def courts_summary(selected: set[str]) -> str:
    if not selected:
        return "Courts: All"
    if len(selected) <= 3:
        return "Courts: " + ", ".join(sorted(selected))
    return f"Courts: {len(selected)} selected"
