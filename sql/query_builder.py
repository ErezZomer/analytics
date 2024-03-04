from abc import ABC, abstractmethod
from analytics.sql.sql_clause import (
    SelectClause,
    FromClause,
    WhereClause,
    GroupByClause,
    OptionCluase,
    OrderByClause,
    AsClause,
    CaseCaluse,
    ConditionExpression,
    ConditionBetweenExpression,
    SubQueryExpression,
)


class QueryBuilder(ABC):
    """A base class for building SQL queries."""

    def __init__(
        self,
        exclude_vehicles: set,
        min_dist: int = 1,
        max_dist: int = 100,
        step_dist: int = 10,
    ) -> None:
        self._query = None
        self._vehicles = exclude_vehicles
        self._min = min_dist
        self._max = max_dist
        self._step = step_dist

    @abstractmethod
    def build_select(self) -> str:
        """An abstract function for building the SELECT clause.

        Returns:
            str: The SELECT clause.
        """

    @abstractmethod
    def build_from(self) -> str:
        """An abstract function for building the FROM clause.

        Returns:
            str: The FROM clause.
        """

    def build_where(self) -> str:
        """A function to override for building the WHERE clause.

        Returns:
            str: The WHERE clause.
        """
        return ""

    def build_group_by(self) -> str:
        """An function to override for building the GROUP BY clause.

        Returns:
            str: The GROUP BY clause.
        """
        return ""

    def build_order_by(self) -> str:
        """An function to override for building the ORDER BY clause.

        Returns:
            str: The ORDER BY clause.
        """
        return ""

    @property
    def query(self) -> str:
        """Returns the actual query.

        Returns:
            str: The whole SQL query.
        """
        return self._query

    def build_query(self):
        """Calls the various sub builds functions and and generate the complete SQL query."""
        query = str()
        select_ = self.build_select()
        from_ = self.build_from()
        where_ = self.build_where()
        group_ = self.build_group_by()
        order_ = self.build_order_by()

        query = f"{select_}\n{from_}\n{where_}\n{group_}\n{order_}\n"
        self._query = query.strip()


class RoundedDistanceQuery(QueryBuilder):
    """A class which implements a query which returns the distances
    rounded to bin first distance.
    """

    def build_select(self):
        """A function for building the SELECT clause.

        Returns:
            str: The SELECT clause.
        """
        option = OptionCluase()
        for i in range(self._min, self._max + 1, self._step):
            condition = ConditionBetweenExpression(
                variable="distance", min_value=i, max_value=i + self._step - 1
            )
            option.add_option(condition.expression, i)
        option.add_alternative(0)
        dist_alias = AsClause("dist")
        option.end_option(dist_alias)
        case = CaseCaluse()
        case.add_case(option)
        case.build()
        select = SelectClause(["vehicle_type", "detection", "distance", case.clause])
        select.build()
        return select.clause

    def build_from(self) -> str:
        """A function for building the FROM clause.

        Returns:
            str: The FROM clause.
        """
        fromc = FromClause("src")
        fromc.build()
        return fromc.clause


class CountedDistancesQuery(QueryBuilder):
    """Builds the query which count the number of rows for
    each of the selected distances (the bins).
    """

    def build_select(self):
        """A function for building the SELECT clause.

        Returns:
            str: The SELECT clause.
        """
        select = SelectClause(["vehicle_type", "dist"])
        dist_alias = AsClause("number_of_dist")
        select.count_aggr("dist", dist_alias)
        detections_alias = AsClause("number_of_detections")
        select.count_if_aggr("detection", detections_alias)
        select.build()
        return select.clause

    def build_from(self):
        """A function for building the FROM clause.

        Returns:
            str: The FROM clause.
        """
        rounded_distances = RoundedDistanceQuery(
            self._vehicles, self._min, self._max, self._step
        )
        rounded_distances.build_query()
        rounded_distances_table = SubQueryExpression(subquery=rounded_distances.query)
        fromc = FromClause(rounded_distances_table.expression)
        fromc.build()
        return fromc.clause

    def build_group_by(self) -> str:
        """An function to override for building the GROUP BY clause.

        Returns:
            str: The GROUP BY clause.
        """
        group = GroupByClause(["vehicle_type", "dist"])
        group.build()
        return group.clause

    def build_order_by(self) -> str:
        """An function to override for building the ORDER BY clause.

        Returns:
            str: The ORDER BY clause.
        """
        order = OrderByClause(["vehicle_type"])
        order.build()
        return order.clause


class TrueDetectionsQuery(QueryBuilder):
    """Builds the query which calculates the amount of detections
    per rows per vehicle type per selected distance (the bins).

    Args:
        QueryBuilder (_type_): _description_
    """

    def build_select(self):
        """A function for building the SELECT clause.

        Returns:
            str: The SELECT clause.
        """
        select = SelectClause(["vehicle_type"])
        for i in range(self._min, self._max + 1, self._step):
            case = CaseCaluse()
            option = OptionCluase()
            condition = ConditionExpression(variable="dist", operator="=", value=f"{i}")
            option.add_option(
                condition.expression, "100.0 * number_of_detections / number_of_dist"
            )
            option.end_option()
            case.add_case(option)
            case.build()
            alias = AsClause(f'"{i}_{i + self._step -1}"')
            select.max_aggr(case.clause, alias)
        select.build()
        return select.clause

    def build_from(self) -> str:
        """A function for building the FROM clause.

        Returns:
            str: The FROM clause.
        """
        counted_distances = CountedDistancesQuery(
            self._vehicles, self._min, self._max, self._step
        )
        counted_distances.build_query()
        counted_distances_table = SubQueryExpression(subquery=counted_distances.query)
        fromc = FromClause(counted_distances_table.expression)
        fromc.build()
        return fromc.clause

    def build_where(self) -> str:
        """An function to override for building the WHERE clause.

        Returns:
            str: The WHERE clause.
        """
        where = WhereClause()
        condition = ConditionExpression(
            variable="vehicle_type", operator="!=", value="'ignore'"
        )
        where.and_condition(condition.expression)
        for vehicle in self._vehicles:
            condition = ConditionExpression(
                variable="vehicle_type", operator="!=", value=f"'{vehicle}'"
            )
            where.and_condition(condition.expression)
        where.build()
        return where.clause

    def build_group_by(self) -> str:
        """An function to override for building the GROUP BY clause.

        Returns:
            str: The GROUP BY clause.
        """
        group = GroupByClause(["vehicle_type"])
        group.build()
        return group.clause

    def build_order_by(self) -> str:
        """An function to override for building the ORDER BY clause.

        Returns:
            str: The ORDER BY clause.
        """
        order = OrderByClause(["vehicle_type"])
        order.build()
        return order.clause
