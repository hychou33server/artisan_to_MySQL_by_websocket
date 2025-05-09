import asyncio
import websockets
import json
import logging
from datetime import datetime
import mysql.connector
from mysql.connector import Error

# 配置日誌系統，記錄程序運行狀態和重要事件
logging.basicConfig(
    level=logging.INFO,  # 設定日誌級別為 INFO，記錄一般運行信息
    format='%(asctime)s - %(levelname)s - %(message)s'  # 日誌格式包含時間、日誌級別和消息
)
logger = logging.getLogger(__name__)  # 創建日誌記錄器

class ETDataReceiver:
    def __init__(self):
        # 初始化類的關鍵屬性
        self.connected_clients = set()  # 存儲當前連接的 WebSocket 客戶端
        self.received_data = []  # 暫存接收到的數據記錄
        self.db_connection = self.create_db_connection()  # 創建初始數據庫連接

    def create_db_connection(self):
        """
        創建與 MySQL 數據庫的連接
        
        Returns:
            mysql.connector.connection: 成功連接時返回連接對象，否則返回 None
        """
        try:
            # 使用 mysql.connector 建立數據庫連接
            connection = mysql.connector.connect(
                host='Your MySQL ip',       # MySQL 服務器地址
                database='Your MySQL db',            # 數據庫名稱
                user='Your MySQL username',    # MySQL 用戶名
                password='Your MySQL password' # MySQL 密碼
            )
            if connection.is_connected():
                logger.info("成功連接到 MySQL 數據庫")
                return connection
        except Error as e:
            # 捕獲並記錄連接錯誤
            logger.error(f"連接 MySQL 數據庫時出錯: {e}")
            return None

    def insert_temperature_data(self, timestamp, bt=None, et=None):
        """
        將溫度數據插入 MySQL 數據庫
        
        Args:
            timestamp (str): 數據記錄的時間戳
            bt (float, optional): Bean Temperature 豆溫
            et (float, optional): Environmental Temperature 環境溫度
        """
        # 檢查數據庫連接是否可用
        if not self.db_connection:
            logger.error("數據庫未連接")
            return

        try:
            # 創建數據庫游標用於執行 SQL 查詢
            cursor = self.db_connection.cursor()
            
            # 定義插入數據的 SQL 語句
            query = """
            INSERT INTO artisan (timestamp, BT, ET) 
            VALUES (%s, %s, %s)
            """
            # 執行 SQL 查詢，將數據插入數據庫
            cursor.execute(query, (timestamp, bt, et))
            
            # 提交事務，確保數據被實際寫入
            self.db_connection.commit()
            logger.info("數據成功寫入數據庫")
        
        except Error as e:
            # 捕獲並記錄插入數據時的錯誤
            logger.error(f"插入數據時出錯: {e}")
            # 嘗試重新建立數據庫連接
            self.db_connection = self.create_db_connection()
        
        finally:
            # 確保游標被關閉，釋放資源
            if 'cursor' in locals():
                cursor.close()

    async def handle_client(self, websocket):
        """
        處理 WebSocket 客戶端連接和消息
        
        Args:
            websocket: WebSocket 連接對象
        """
        # 為每個客戶端生成唯一標識符
        client_id = str(id(websocket))
        
        # 將客戶端添加到已連接客戶端集合
        self.connected_clients.add(websocket)
        logger.info(f"新客戶端連接。ID: {client_id}")
        
        try:
            # 持續監聽來自客戶端的消息
            async for message in websocket:
                try:
                    # 解析接收到的 JSON 消息
                    data = json.loads(message)
                    
                    # 生成當前時間戳
                    timestamp = datetime.now().isoformat()
                    
                    # 從接收的數據中提取 BT 和 ET
                    bt = data.get('BT')
                    et = data.get('ET')
                    
                    # 將數據記錄到數據庫
                    self.insert_temperature_data(timestamp, bt, et)
                    
                    # 創建數據記錄，用於本地暫存
                    data_record = {
                        "timestamp": timestamp,
                        "client_id": client_id,
                        "data": data
                    }
                    
                    # 將數據記錄添加到本地列表
                    self.received_data.append(data_record)
                    logger.info(f"接收到數據: {data}")
                    
                    # 發送成功接收的響應
                    response = {
                        "status": "success",
                        "message": "數據已接收",
                        "timestamp": timestamp
                    }
                    await websocket.send(json.dumps(response))
                    
                except json.JSONDecodeError:
                    # 處理 JSON 解析錯誤
                    logger.error(f"無效的 JSON 格式: {message}")
                    await websocket.send(json.dumps({
                        "status": "error",
                        "message": "無效的數據格式"
                    }))
                
        except websockets.exceptions.ConnectionClosed:
            # 處理客戶端連接斷開的情況
            logger.info(f"客戶端斷開連接。ID: {client_id}")
        finally:
            # 確保客戶端從連接集合中移除
            self.connected_clients.remove(websocket)

    def get_received_data(self):
        """
        獲取本地暫存的接收數據列表
        
        Returns:
            list: 接收到的數據記錄
        """
        return self.received_data

    async def start_server(self, host, port):
        """
        啟動 WebSocket 服務器
        
        Args:
            host (str): 服務器監聽的主機地址
            port (int): 服務器監聽的端口
        
        Returns:
            websockets.Server: WebSocket 服務器對象
        """
        server = await websockets.serve(
            lambda websocket: self.handle_client(websocket),
            host, 
            port
        )
        logger.info(f"WebSocket 服務器已啟動於 {host}:{port}")
        return server

    def __del__(self):
        """
        物件銷毀時關閉數據庫連接
        確保資源被正確釋放
        """
        if hasattr(self, 'db_connection') and self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
            logger.info("數據庫連接已關閉")

async def main():
    """
    主程序入口，初始化並運行 WebSocket 服務器
    """
    receiver = ETDataReceiver()
    server = await receiver.start_server("192.168.0.111", 8765)
    
    try:
        await asyncio.Future()  # 使服務器持續運行
    except KeyboardInterrupt:
        # 處理鍵盤中斷信號（Ctrl+C）
        logger.info("服務器關閉中...")
        server.close()
        await server.wait_closed()
        logger.info("服務器已關閉")

if __name__ == "__main__":
    # 主程序入口點，運行異步 main 函數
    
    asyncio.run(main())
