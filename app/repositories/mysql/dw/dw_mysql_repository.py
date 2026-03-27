from typing import Dict

from sqlalchemy import text, Result
from sqlalchemy.ext.asyncio import AsyncSession


class DWMySQLRepository:
    """用于操作dw数据库持久层"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_column_types(self, table_name: str) -> dict[str, str]:
        """根据表名获取该下所有字段类型"""
        sql = f"show COLUMNS from {table_name}"
        result: Result = await self.session.execute(text(sql))
        # 自定义SQL 结果获取通过result.fetchall()函数获取
        return {row.Field: row.Type for row in result.fetchall()}

    async def get_column_values(self, table_name:str, column_name:str, limit:int=10):
        sql = f"SELECT DISTINCT {column_name} from {table_name} limit {limit}"
        result: Result = await self.session.execute(text(sql))
        return result.scalars().fetchall()

    async def get_db_info(self) ->dict[str, str]:
        #1.查询数据库管理系统方言（名称）
        dialect = self.session.get_bind().dialect.name
        #2.查询数据库管理系统版本信息
        result:Result = await self.session.execute(text("SELECT version()"))
        version = result.scalar()
        return {"dialect": dialect, "version": version}

    async def validate_sql(self, sql):
        await self.session.execute(text(f"explain {sql}"))

    async def execute_sql(self, sql:str):
        result:Result = await self.session.execute(text(sql))
        return [dict(**row_mapping) for row_mapping in  result.mappings().fetchall()]
