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
    SubQueryExpression
)


class QueryBuilder(ABC):
    def __init__(self) -> None:
        self._query = None

    @abstractmethod
    def build_select(self) -> str:
        pass

    @abstractmethod
    def build_from(self) -> str:
        pass

    def build_where(self) -> str:
        return ""

    def build_group_by(self) -> str:
        return ""

    def build_order_by(self) -> str:
        return ""

    @property
    def query(self) -> str:
        return self._query

    def build_query(self, *args, **kwargs):
        query = str()
        select_ = self.build_select(*args, **kwargs)
        from_ = self.build_from(*args, **kwargs)
        where_= self.build_where(*args, **kwargs)
        group_= self.build_group_by(*args, **kwargs)
        order_= self.build_order_by(*args, **kwargs)

        query = f"{select_}\n{from_}\n{where_}\n{group_}\n{order_}\n"
        self._query = query.strip()


class RounderDistanceQuery(QueryBuilder):
    def build_select(self, min_distance: int = 1, max_distance: int = 100, step: int = 10):
        option = OptionCluase()
        for i in range(min_distance, max_distance + 1, step):
            condition = ConditionBetweenExpression(variable="distance", min_value=i, max_value=i + step - 1)
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
    
    def build_from(self, *args, **kwargs) -> str:
        fromc = FromClause("src")
        fromc.build()
        return fromc.clause

class CountedDistancesQuery(QueryBuilder):
    def build_select(self, *args, **kwargs):
        select = SelectClause(["vehicle_type", "dist"])
        dist_alias = AsClause("number_of_dist")
        select.count_aggr("dist", dist_alias)
        detections_alias = AsClause("number_of_detections")
        select.count_if_aggr("detection", detections_alias)
        select.build()
        return select.clause
    
    def build_from(self, *args, **kwargs):
        rounded_distances = RounderDistanceQuery()
        rounded_distances.build_query()
        rounded_distances_table = SubQueryExpression(subquery=rounded_distances.query)
        fromc = FromClause(rounded_distances_table.expression)
        fromc.build()
        return fromc.clause
        

    def build_group_by(self, *args, **kwargs) -> str:
        group = GroupByClause(["vehicle_type", "dist"])
        group.build()
        return group.clause

    def build_order_by(self, *args, **kwargs) -> str:
        order = OrderByClause(["vehicle_type"])
        order.build()
        return order.clause


class TrueDetectionsQuery(QueryBuilder):
    def build_select(self, min_distance: int = 1, max_distance: int = 100, step: int = 10):
        select = SelectClause(["vehicle_type"])
        for i in range(min_distance, max_distance + 1, step):
            case = CaseCaluse()
            option = OptionCluase()
            condition = ConditionExpression(variable="dist", operator="=", value=f"{i}")
            option.add_option(condition.expression, "100.0 * number_of_detections / number_of_dist")
            option.end_option()
            case.add_case(option)
            case.build()
            alias = AsClause(f"\"{i}_{i + step -1}\"")
            select.max_aggr(case.clause, alias)
        select.build()
        return select.clause

    def build_from(self, *args, **kwargs) -> str:
        counted_distances = CountedDistancesQuery()
        counted_distances.build_query()
        counted_distances_table = SubQueryExpression(subquery=counted_distances.query)
        fromc = FromClause(counted_distances_table.expression)
        fromc.build()
        return fromc.clause

    def build_where(self) -> str:
        where = WhereClause()
        condition = ConditionExpression(variable="vehicle_type", operator="!=", value="'ignore'")
        where.and_condition(condition.expression)
        where.build()
        return where.clause

    def build_group_by(self, *args, **kwargs) -> str:
        group = GroupByClause(["vehicle_type"])
        group.build()
        return group.clause

    def build_order_by(self, *args, **kwargs) -> str:
        order = OrderByClause(["vehicle_type"])
        order.build()
        return order.clause
