'''
    PM4Py – A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschränkt)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see this software project's root or
visit <https://www.gnu.org/licenses/>.

Website: https://processintelligence.solutions
Contact: info@processintelligence.solutions
'''
class Trie(object):

    def __init__(
        self, label=None, parent=None, children=None, final=False, depth=0
    ):
        self._label = label
        self._parent = parent
        self._children = children if children is not None else list()
        self._final = final
        self._depth = depth

    def _set_parent(self, parent):
        self._parent = parent

    def _set_label(self, label):
        self._label = label

    def _set_children(self, children):
        self._children = children

    def _set_final(self, final):
        self._final = final

    def _get_children(self):
        return self._children

    def _get_final(self):
        return self._final

    def _get_parent(self):
        return self._parent

    def _get_label(self):
        return self._label

    def _set_depth(self, depth):
        self._depth = depth

    def _get_depth(self):
        return self._depth

    parent = property(_get_parent, _set_parent)
    children = property(_get_children, _set_children)
    label = property(_get_label, _set_label)
    final = property(_get_final, _set_final)
    depth = property(_get_depth, _set_depth)

    def repr_trie(self, indent_level=0):
        stri = []

        if self.label:
            stri.append("\t" * indent_level + self.label)
            indent_level += 1
        for child in self.children:
            stri.append(child.repr_trie(indent_level=indent_level))
        if self.final:
            stri.append("\t" * indent_level + "-- END --")

        return "\n".join(stri)

    def __repr__(self):
        return self.repr_trie()

    def __str__(self):
        return self.repr_trie()

    def __eq__(self, other):
        if not isinstance(other, Trie):
            return False
        if self._label != other._label:
            return False
        if self._final != other._final:
            return False
        this_children = sorted(self._children, key=lambda x: x._label)
        other_children = sorted(other._children, key=lambda x: x._label)
        if len(this_children) != len(other_children):
            return False
        for i in range(len(this_children)):
            if this_children[i] != other_children[i]:
                return False
        return True

    def __hash__(self):
        return hash(
            (
                self._label,
                self._final,
                tuple(sorted(self._children, key=lambda x: x._label)),
            )
        )
