import oss2
from configparser import ConfigParser
import os
import pandas as pd

class OSSClient:
    def __init__(self):
        config = ConfigParser()
        config.read('config/settings.ini')

        self.auth = oss2.Auth(
            config['OSS']['ACCESS_KEY_ID'],
            config['OSS']['ACCESS_KEY_SECRET']
        )
        self.bucket = oss2.Bucket(
            self.auth,
            config['OSS']['ENDPOINT'],
            config['OSS']['BUCKET_NAME']
        )

    def upload_file(self, local_path, object_key):
        """上传文件到OSS"""
        try:
            self.bucket.put_object_from_file(object_key, local_path)
            print(f"上传成功: {object_key}")
            return True
        except Exception as e:
            print(f"上传失败: {str(e)}")
            return False

    def download_file(self, object_key, local_path):
        """从OSS下载文件"""
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.bucket.get_object_to_file(object_key, local_path)
            print(f"下载成功: {object_key}")
            return True
        except Exception as e:
            print(f"下载失败: {str(e)}")
            return False

    def preprocess_data(self, local_path):
        """数据预处理：统一时间格式、价格单位等"""
        try:
            df = pd.read_csv(local_path)

            # 统一时间格式
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

            # 统一价格单位（假设原始数据可能是分，转换为元）
            if 'price' in df.columns:
                df['price'] = df['price'] / 100.0  # 如果原始数据是分

            # 保存处理后的数据
            processed_path = local_path.replace('.csv', '_processed.csv')
            df.to_csv(processed_path, index=False)
            return processed_path
        except Exception as e:
            print(f"数据预处理失败: {str(e)}")
            return local_path  # 如果预处理失败，返回原始文件