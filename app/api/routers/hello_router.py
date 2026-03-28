from fastapi import APIRouter, Depends
from fastapi import  Query, Path
from pydantic import BaseModel
# 创建路由对象
hello_router = APIRouter(prefix="/api", tags=["路由入门案例"])


#2.创建用于测试Http接口(路由)  查询参数：/hello?name=xxx 路径参数：/{address}
@hello_router.get("/hello/{address}")
async def hello(name:str=Query(min_length=2, max_length=6), address:str=Path(min_length=3, max_length=6)):
    return {"message":f"hello {name}, 来自{address}"}

# {name:"张三",mobile:"xxxx"}

class Item(BaseModel):
    name: str
    mobile: str | None = None


# 请求提参数
@hello_router.post("/register")
async def register(item:Item):
    return {"message":f"{item} 注册成功"}


async def get_config():
    print("子依赖被调用")
    return {"ip": "192.168.200.10"}


async def p1(config:dict = Depends(get_config, use_cache=False)):
    return config

async def p2(config:dict = Depends(get_config, use_cache=False)):
    return config

# 会不会使用缓存前提条件是：当前一次请求内
@hello_router.post("/test_depends")
async def test_depends(p1:dict = Depends(p1), p2:dict = Depends(p2)):
    return {"p1":p1, "p2":p2}