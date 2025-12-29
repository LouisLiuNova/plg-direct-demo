# 用于mocking服务的容器配置
FROM python:3.12-slim
WORKDIR /app
COPY mock_async.py .
# -u 参数保证日志不缓存，实时输出
CMD ["python", "-u", "mock_async.py"]
