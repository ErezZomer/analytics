from typing import List

from pydantic import BaseModel


class ConditionExpression(BaseModel):
    """A class which represnts an expression that might be used inside SQL clauses."""

    variable: str
    operator: str
    value: str

    @property
    def expression(self) -> str:
        """A propery to display the expression in the correct order.

        Returns:
            str: The full expression.
        """
        return f"{self.variable} {self.operator} {self.value}"


class ConditionBetweenExpression(BaseModel):
    """A class which represnt a BETWEEN expression that might be used inside SQL clauses."""

    variable: str
    min_value: int
    max_value: int

    @property
    def expression(self) -> str:
        """A propery to display the expression in the correct order.

        Returns:
            str: The full expression.
        """
        return f"{self.variable} BETWEEN {self.min_value} AND {self.max_value}"


class SubQueryExpression(BaseModel):
    """Encapsulates subquery inside parentheses."""

    subquery: str

    @property
    def expression(self) -> str:
        """A propery which returns the subquery inside parentheses.

        Returns:
            str: The (subquery).
        """
        return f"({self.subquery})"


class SqlClause(BaseModel):
    """A Base class for representing SQL clause."""

    command: str
    clause: str = ""

    def build(self):
        """A function to override with derived classes.
        This fucntion should build SQL clause of particular object.
        """


class AsClause(SqlClause):
    """A class for adding alias to SQL."""

    alias: str

    def __init__(self, alias: str) -> None:
        """Ctor

        Args:
            alias (str): The name of the alias to use.
        """
        super().__init__(command="AS", alias=alias)

    def build(self):
        """Builds the SQL Clause and store it in self.clause."""
        self.clause = f"{self.command} {self.alias}"


class SelectClause(SqlClause):
    """This class implements the SELECT clause of SQL."""

    fields: list = []

    def __init__(self, fields: list) -> None:
        super().__init__(command="SELECT", fields=fields)

    def build(self):
        """Builds the SQL Clause and store it in self.clause."""
        fields = ",\n\t".join(self.fields)
        self.clause = f"{self.command} {fields}"

    def _aggr_func(self, func: str, field, alias: AsClause = None):
        """An internal function which is used by aggregation functions."""
        field = f"{func}({field})"
        if alias:
            alias.build()
            field += f" {alias.clause}"
        self.fields.append(field)

    def count_aggr(self, field: str, alias: AsClause = None):
        """Adding count(field) to SQL clause.

        Args:
            field (str): The name of the field to count
            alias (AsClause, optional): ads and aliasing. Defaults to None.
        """
        self._aggr_func("count", field, alias)

    def count_if_aggr(self, field: str, alias: AsClause):
        """Adding count_if(field) to SQL clause.

        Args:
            field (str): The name of the field to count
            alias (AsClause, optional): ads and aliasing. Defaults to None.
        """
        self._aggr_func("count_if", field, alias)

    def sum_aggr(self, field, alias: AsClause):
        """Adding sum(field) to SQL clause.

        Args:
            field (str): The name of the field to sum
            alias (AsClause, optional): ads and aliasing. Defaults to None.
        """
        self._aggr_func("sum", field, alias)

    def max_aggr(self, field, alias: AsClause):
        """Adding max(field) to SQL clause.

        Args:
            field (str): The name of the field to maximize
            alias (AsClause, optional): ads and aliasing. Defaults to None.
        """
        self._aggr_func("max", field, alias)

    def min_aggr(self, field, alias: AsClause):
        """Adding min(field) to SQL clause.

        Args:
            field (str): The name of the field to minimize.
            alias (AsClause, optional): ads and aliasing. Defaults to None.
        """
        self._aggr_func("min", field, alias)

    def averge_aggr(self, field, alias: AsClause):
        """Adding count(field) to SQL clause.

        Args:
            field (str): The name of the field to average.
            alias (AsClause, optional): ads and aliasing. Defaults to None.
        """
        self._aggr_func("avg", field, alias)


class OptionCluase(SqlClause):
    """This class implements the WHEN clause inside CASE of SQL."""

    options: list = []

    def __init__(
        self,
    ) -> None:
        """Ctor."""
        super().__init__(command="WHEN")

    def build(self):
        """Builds the SQL Clause and store it in self.clause."""
        self.clause = "\n".join(self.options)

    def add_option(self, condition: str, value: str):
        """Adds an options to the case clause.

        Args:
            condition (str): The condition expression.
            value (str): The value to use in case condition is True.
        """
        condition = f"{self.command} {condition} THEN {value}"
        self.options.append(condition)

    def add_alternative(self, alternative: str):
        """The alternative to all options in a CASE clause.

        Args:
            alternative (str): The alternative value to use if no condition is met.
        """
        self.options[-1] += f"\nELSE {alternative}"

    def end_option(self, alias: AsClause = None):
        """Ends the CASE clasue.

        Args:
            alias (AsClause, optional): adds aliassing to CASE clause. Defaults to None.
        """
        end = " END"
        if alias:
            alias.build()
            end = f" END {alias.clause}"

        if self.options:
            if len(self.options) > 1:
                end = end.strip()
                end = f"\n{end}"
            self.options[-1] += end


class CaseCaluse(SqlClause):
    """This class implements the CASE clause of SQL."""

    cases: list = []

    def __init__(
        self,
    ) -> None:
        super().__init__(command="CASE")

    def build(self):
        """Builds the SQL Clause and store it in self.clause."""
        cases = ",\n".join(self.cases)

        if len(self.cases) > 1:
            self.clause = f"\n{self.command}\n{cases}"
        else:
            self.clause = f"{self.command} {cases}"

    def add_case(self, case: OptionCluase):
        """Adds an option to the CASE clause.

        Args:
            case (OptionCluase): The options to add.
        """
        case.build()
        self.cases.append(case.clause)


class FromClause(SqlClause):
    """This class implements the FROM clause of SQL."""

    table: str

    def __init__(self, table: str) -> None:
        """Ctor.

        Args:
            table (str): The name of the table to query data from.
        """
        super().__init__(command="FROM", table=table)

    def build(self):
        """Builds the SQL Clause and store it in self.clause."""
        self.clause = f"{self.command}\n\t{self.table}"


class WhereClause(SqlClause):
    """This class implements the WHERE clause of SQL."""

    conditions: list = []

    def __init__(self) -> None:
        """Ctor."""
        super().__init__(command="WHERE")

    def and_condition(self, condition: str):
        """Adds an AND condition to the WHERE clause.

        Args:
            condition (str): The condition to add.
        """
        if self.conditions:
            condition = "and " + condition
        self.conditions.append(condition)

    def or_condition(self, condition: str):
        """Adds an OR condition to the WHERE clause.

        Args:
            condition (str): The condition to add.
        """

        if self.conditions:
            condition = "or " + condition
        self.conditions.append(condition)

    def build(self):
        """Builds the SQL Clause and store it in self.clause."""
        clause = " ".join(self.conditions)
        self.clause = f"{self.command} {clause}"


class GroupByClause(SqlClause):
    """This class implements the GROUP BY clause of SQL."""

    fields: List[str] = []

    def __init__(self, fields: list) -> None:
        """_summary_

        Args:
            fields (list): _description_
        """
        super().__init__(command="GROUP BY", fields=fields)

    def build(self):
        """Builds the SQL Clause and store it in self.clause."""
        fields = ",\n\t".join(self.fields)
        self.clause = f"{self.command}\n\t{fields}"


class OrderByClause(SqlClause):
    """This class implements the ORDER BY clause of SQL."""

    fields: list = []

    def __init__(self, fields: list) -> None:
        """Ctor.

        Args:
            fields (list): The fields to use in the ORDER BY clause.
        """
        super().__init__(command="ORDER BY", fields=fields)

    def build(self):
        """Builds the SQL Clause and store it in self.clause."""
        fields = ",\n\t".join(self.fields)
        self.clause = f"{self.command}\n\t{fields}"
