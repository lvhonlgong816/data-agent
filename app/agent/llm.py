import asyncio

from langchain.chat_models import init_chat_model
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    model="deepseek-chat",
    temperature=0,
    api_key="sk-6b75e53f512c46eb9a9087dfc892defa",
    base_url="https://api.deepseek.com"
)

# llm = init_chat_model(
#     model="gpt-5.4-mini",
#     model_provider="openai",
# )

if __name__ == '__main__':
    async def test():
        result = await llm.ainvoke("简单介绍自己")
        print(result.content)

    asyncio.run(test())