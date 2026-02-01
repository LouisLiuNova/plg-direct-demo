import pymysql

try:
    # 配置和命令行mysql完全一致
    conn = pymysql.connect(
        host='127.0.0.1',
        port=4000,
        user='root',
        password='',  # 无密码留空
        database='mysql',  # 命令行中已存在的mysql数据库
        charset='utf8mb4',
        connect_timeout=5,  # 5秒超时
        ssl=None  # 强制关闭SSL，避免TiDB未开启SSL导致连接失败
    )
    print("✅ pymysql 直接连接 TiDB 成功")
    conn.close()
except Exception as e:
    print(f"❌ pymysql 连接失败：{e}")
