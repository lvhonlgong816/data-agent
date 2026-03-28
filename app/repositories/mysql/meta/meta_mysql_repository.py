from sqlalchemy import select, ScalarResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.entities.column_info import ColumnInfo
from app.entities.column_metric import ColumnMetric
from app.entities.metric_info import MetricInfo
from app.entities.table_info import TableInfo
from app.models.column_info_mysql import ColumnInfoMySQL
from app.models.table_info_mysql import TableInfoMySQL
from app.repositories.mysql.meta.mappers.column_info_mapper import ColumnInfoMapper
from app.repositories.mysql.meta.mappers.column_metric_mapper import ColumnMetricMapper
from app.repositories.mysql.meta.mappers.metric_info_mapper import MetricInfoMapper
from app.repositories.mysql.meta.mappers.table_info_mapper import TableInfoMapper


class MetaMySQLRepository:
    """用于操作meta数据库"""

    def __init__(self, session: AsyncSession|None):
        self.session = session

    async def save_table_infos(self, table_infos: list[TableInfo]):
        # 1. 将业务对象转为ORM模型对象
        models = [TableInfoMapper.to_model(table_info) for table_info in table_infos]
        # 2. 调用框架提供批量保存函数
        self.session.add_all(models)

    async def save_column_infos(self, column_infos: list[ColumnInfo]):
        models = [ColumnInfoMapper.to_model(column_info) for column_info in column_infos]
        self.session.add_all(models)

    async def save_metric_infos(self, metric_infos: list[MetricInfo]):
        models = [MetricInfoMapper.to_model(metric_info) for metric_info in metric_infos]
        self.session.add_all(models)

    async def save_column_metric_infos(self, column_metric_infos: list[ColumnMetric]):
        models = [ColumnMetricMapper.to_model(column_metric_info) for column_metric_info in column_metric_infos]
        self.session.add_all(models)

    async def get_column_info(self, column_id) -> ColumnInfo:
        """根据字段ID查询字段信息"""
        column_info_mysql: ColumnInfoMySQL | None = await self.session.get(ColumnInfoMySQL, column_id)
        return ColumnInfoMapper.to_entity(column_info_mysql)

    async def get_key_columns(self, table_id: str) -> list[ColumnInfo]:
        # sql = select * from column_info where table_id = ? and role in ("primary_key","foreign_key")
        stmt = select(ColumnInfoMySQL).where(ColumnInfoMySQL.table_id == table_id,
                                             ColumnInfoMySQL.role.in_(["primary_key", "foreign_key"]))
        # ORM方式执行查库操作，底层将多列结果封装到一个对象中，scalars处理是单个元素
        result: ScalarResult = await self.session.scalars(stmt)
        return [ColumnInfoMapper.to_entity(column_info_mysql) for column_info_mysql in result]

    async def get_table_info(self, table_id:str) -> TableInfo:
        table_info_mysql:TableInfoMySQL|None = await self.session.get(TableInfoMySQL, table_id)
        return TableInfoMapper.to_entity(table_info_mysql)
