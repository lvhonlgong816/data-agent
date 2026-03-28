import asyncio

from fastapi import APIRouter, Depends

from app.api.dependencies import get_query_service
from app.api.schemas.query_schema import QuerySchema
from fastapi.responses import StreamingResponse

from app.services.query_service import QueryService

query_router = APIRouter(prefix="/api", tags=["问数路由"])

@query_router.post("/query")
async def query(query:QuerySchema, query_service:QueryService = Depends(get_query_service)):
    """
     前后端约定提交参数 采用请求体：{"query":"统计各分类销售商品数量"}
    :param query: 问题
    :return: 异步流式返回
        进度展示 约定{type:"progress",step:"找回字段","status":"running"}
        数据展示 约定{type:"result","data":业务数据}
        sse协议：data: 数据 \n\n
    """
    return StreamingResponse(query_service.query_answer(query.query), media_type="text/event-stream")






@query_router.post("/steam")
async def steam_test():
    """
    :return: 异步流式返回
        SSE协议规范数据 data: 数字1~9
    """
    return StreamingResponse(return_num(), media_type="text/event-stream")

async def return_num():
    for i in range(1,10):
        await asyncio.sleep(1)
        yield f"data: 数字{i}\n\n"


async def return_num():
    for i in range(1,10):
        await asyncio.sleep(1)
        yield f"data: 数字{i}\n\n"

if __name__ == '__main__':
    # 定义生成器函数
    def my_generator():
        yield 1  # 第一次调用返回 1，暂停+记录位置+继续执行
        yield 2  # 第二次调用返回 2，暂停
        yield 3  # 第三次调用返回 3，暂停

    # 创建生成器对象
    gen = my_generator()
    print(next(gen))  # 输出：1
    print(next(gen))  # 输出：2
    print(next(gen))  # 输出：3
