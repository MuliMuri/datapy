"""
@File    :   builder.py
@Time    :   2025/04/21 15:31:12
@Author  :   MuliMuri
@Version :   1.0
@Desc    :   SQL Query Builder with Natural Expression Support

Features:
- Natural ConditionNode building using Python operators
- Safe parameterization
- Pagination and sorting
- Operator overloading for AND/OR/NOT
"""


from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Sequence


class Field():
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value: Any):
        raise AssertionError("Cannot modify read-only attribute, define the value of this field only at initialization.")

    def __eq__(self, value: Any) -> 'ASTBasicNode':
        return OPComparisonNode(self, '=', value)

    def __ne__(self, value: Any) -> 'ASTBasicNode':
        return OPComparisonNode(self, '!=', value)

    def __lt__(self, value: Any) -> 'ASTBasicNode':
        return OPComparisonNode(self, '<', value)

    def __le__(self, value: Any) -> 'ASTBasicNode':
        return OPComparisonNode(self, '<=', value)

    def __gt__(self, value: Any) -> 'ASTBasicNode':
        return OPComparisonNode(self, '>', value)

    def __ge__(self, value: Any) -> 'ASTBasicNode':
        return OPComparisonNode(self, '>=', value)

    def is_in(self, value: Sequence[Any]) -> 'ASTBasicNode':
        return OPInNode(self, value)

    def __container__(self, value: Sequence[Any]) -> 'ASTBasicNode':
        return OPInNode(self, value)

    def between(self, low: Any, high: Any) -> 'ASTBasicNode':
        return OPBetweenNode(self, (low, high))


class ASTBasicNode(ABC):
    def __and__(self, other: 'ASTBasicNode') -> 'OPLogicalNode':
        return OPLogicalNode('AND', [self, other])

    def __or__(self, other: 'ASTBasicNode') -> 'OPLogicalNode':
        return OPLogicalNode('OR', [self, other])

    def __invert__(self) -> 'OPLogicalNode':
        return OPLogicalNode('NOT', [self])

    @abstractmethod
    def compile(self) -> Tuple[str, Tuple[Any]]:
        raise NotImplementedError()


class OPComparisonNode(ASTBasicNode):
    def __init__(self,
                 field: Field,
                 operator: str,
                 value: Any
                 ):

        self.field = field
        self.operator = operator
        self.value = value

    def compile(self) -> Tuple[str, Tuple[Any]]:
        return f"{self.field.name} {self.operator} %s", (self.value, )


class OPInNode(ASTBasicNode):
    def __init__(self,
                 field: Field,
                 values: Sequence[Any]
                 ):

        self.field = field
        self.values = values

    def compile(self) -> Tuple[str, Tuple[Any]]:
        placeholders = ', '.join(['%s'] * len(self.values))
        return f"{self.field.name} IN ({placeholders})", tuple(self.values)


class OPBetweenNode(ASTBasicNode):
    def __init__(self,
                 field: Field,
                 val_range: Tuple[Any, Any]
                 ):

        self.field = field
        self.val_range = val_range

    def compile(self) -> Tuple[str, Tuple[Any]]:
        return f"{self.field.name} BETWEEN %s AND %s", self.val_range


class OPLogicalNode(ASTBasicNode):
    def __init__(self,
                 operator: str,
                 child_nodes: List[ASTBasicNode]
                 ):

        self.operator = operator
        self.child_nodes = child_nodes

    def __and__(self, other: 'ASTBasicNode') -> 'OPLogicalNode':
        if (self.operator == 'AND'):
            return OPLogicalNode('AND', self.child_nodes + [other])

        return OPLogicalNode('AND', [self, other])

    def __or__(self, other: 'ASTBasicNode') -> 'OPLogicalNode':
        if (self.operator == 'OR'):
            return OPLogicalNode('OR', self.child_nodes + [other])

        return OPLogicalNode('OR', [self, other])

    def compile(self) -> Tuple[str, Tuple[Any]]:
        # Special handling for NOT operator
        if (self.operator == 'NOT'):
            child_sql, child_params = self.child_nodes[0].compile()
            return f"NOT ({child_sql})", child_params

        # Process AND/OR operators
        parts = []
        params = []

        for child_node in self.child_nodes:
            child_sql, child_params = child_node.compile()
            parts.append(child_sql)
            params.extend(child_params)

        # Apply parentheses for multi-child conditions
        combined = f" {self.operator} ".join(parts)
        if (len(parts) > 1):
            combined = f"({combined})"

        return combined, tuple(params)


class SelectBuilder():
    def __init__(self):
        self._select = []
        self._from_table = None
        self._where = None
        self._order_by = []
        self._limit = None
        self._offset = None

    def select(self, *fields: str) -> 'SelectBuilder':
        self._select.extend(fields)
        return self

    def from_table(self, table: str) -> 'SelectBuilder':
        self._from_table = table
        return self

    def where(self, condition: 'ASTBasicNode') -> 'SelectBuilder':
        self._where = condition
        return self

    def order_by(self, field: str, direction: str = 'ASC') -> 'SelectBuilder':
        self._order_by.append(f"{field} {direction.upper()}")
        return self

    def limit(self, count: int) -> 'SelectBuilder':
        self._limit = count
        return self

    def offset(self, start: int) -> 'SelectBuilder':
        self._offset = start
        return self

    def build(self) -> Tuple[str, Tuple[Any]]:
        # Validate required components
        if not self._from_table:
            raise ValueError("FROM clause is required")

        # Build SELECT clause
        select_clause = "SELECT " + (
            ", ".join(self._select) if self._select else "*"
        )

        # Build base query
        sql = [select_clause, f"FROM `{self._from_table}`"]
        params = []

        # Add WHERE clause
        if self._where:
            where_sql, where_params = self._where.compile()
            sql.append(f"WHERE {where_sql}")
            params.extend(where_params)

        # Add ORDER BY
        if self._order_by:
            sql.append(f"ORDER BY {', '.join(self._order_by)}")

        # Add pagination
        if self._limit is not None:
            sql.append("LIMIT %s")
            params.append(self._limit)
        if self._offset is not None:
            sql.append("OFFSET %s")
            params.append(self._offset)

        return " ".join(sql), tuple(params)
