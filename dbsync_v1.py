#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MySQL数据库同步工具 - 从源数据库提取数据并推送到目标数据库
"""

import pymysql
import time
import logging
import argparse
import configparser
import os
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    # 同时将日志输出到文件和控制台
    handlers=[
        logging.FileHandler("db_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("db_sync")


class DatabaseSync:
    def __init__(self, source_config, target_config, tables=None, sync_interval=60):
        """
        初始化数据库同步工具
        Args:
            source_config (dict): 源数据库配置
            target_config (dict): 目标数据库配置
            tables (list): 需要同步的表列表，None表示同步所有表
            sync_interval (int): 同步间隔，单位为秒
        """
        self.source_config = source_config
        self.target_config = target_config
        self.tables = tables
        self.sync_interval = sync_interval
        self.source_conn = None
        self.target_conn = None

    def connect_databases(self):
        """连接源数据库和目标数据库"""
        try:
            # 连接源数据库
            self.source_conn = pymysql.connect(
                host=self.source_config['host'],
                port=int(self.source_config['port']),
                user=self.source_config['user'],
                password=self.source_config['password'],
                database=self.source_config['database'],
                charset='utf8mb4'   # utf8mb4是一个支持多字节字符集的Unicode编码，能够存储包括表情符号在内的所有 Unicode 字符
            )
            logger.info(f"成功连接到源数据库 {self.source_config['host']}:{self.source_config['port']}")

            # 连接目标数据库
            self.target_conn = pymysql.connect(
                host=self.target_config['host'],
                port=int(self.target_config['port']),
                user=self.target_config['user'],
                password=self.target_config['password'],
                database=self.target_config['database'],
                charset='utf8mb4'
            )
            logger.info(f"成功连接到目标数据库 {self.target_config['host']}:{self.target_config['port']}")
            return True
        except Exception as e:
            logger.error(f"连接数据库失败: {str(e)}")
            return False

    def get_tables(self):
        """获取源数据库中的所有表"""
        if self.tables:
            return self.tables
        try:
            # 创建一个游标执行数据库查询
            with self.source_conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                # 返回一个列表，其中的每个元素是一个元组
                tables = [table[0] for table in cursor.fetchall()]
                return tables
        except Exception as e:
            logger.error(f"获取表列表失败: {str(e)}")
            return []

    def get_table_structure(self, table):
        """获取表结构, 返回指定表的创建语句"""
        try:
            with self.source_conn.cursor() as cursor:
                cursor.execute(f"SHOW CREATE TABLE {table}")
                create_statement = cursor.fetchone()[1]
                return create_statement
        except Exception as e:
            logger.error(f"获取表 {table} 结构失败: {str(e)}")
            return None

    def ensure_table_exists(self, table, create_statement):
        """确保目标数据库中表存在，如果不存在则创建"""
        try:
            with self.target_conn.cursor() as cursor:
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                if not cursor.fetchone():
                    logger.info(f"在目标数据库中创建表 {table}")
                    cursor.execute(create_statement)
                    # 提交操作，确保创建生效
                    self.target_conn.commit()
                    return True
                return True
        except Exception as e:
            logger.error(f"确保表 {table} 存在失败: {str(e)}")
            return False

    def get_row_count(self, table):
        """获取表中的行数"""
        try:
            with self.source_conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"获取表 {table} 行数失败: {str(e)}")
            return 0

    def sync_table_data(self, table, batch_size=1000):
        """同步单个表的数据"""
        try:
            # 获取表结构并确保目标表存在
            create_statement = self.get_table_structure(table)
            if not create_statement or not self.ensure_table_exists(table, create_statement):
                return False

            # 获取行数
            total_rows = self.get_row_count(table)
            if total_rows == 0:
                logger.info(f"表 {table} 没有数据，跳过同步")
                return True

            # 先清空目标表数据
            with self.target_conn.cursor() as cursor:
                # 删除表中所有数据，但不删除表结构，表结构仍然存在，不可撤销的：一旦执行，数据将永久丢失
                cursor.execute(f"TRUNCATE TABLE {table}")
                self.target_conn.commit()

            # 分批同步数据
            offset = 0
            with self.source_conn.cursor() as source_cursor:
                while offset < total_rows:
                    # 批量加载数据，避免一次性加载所有数据导致内存占用过大，offset 控制了查询的起始位置，batch_size 控制了每次查询的行数
                    source_cursor.execute(f"SELECT * FROM {table} LIMIT {offset}, {batch_size}")
                    """
                    fetchall() 会一次性把所有结果加载到内存中，因此它适合用于结果集较小的查询。
                    对于非常大的结果集，使用 fetchmany() 或者在迭代游标时处理每一行数据会更节省内存。
                    """
                    rows = source_cursor.fetchall()
                    # print(type(rows))
                    if not rows:
                        break

                    # 获取字段信息
                    field_count = len(source_cursor.description)
                    placeholders = ', '.join(['%s'] * field_count)

                    # 插入数据到目标表
                    with self.target_conn.cursor() as target_cursor:
                        for row in rows:
                            try:
                                target_cursor.execute(f"INSERT INTO {table} VALUES ({placeholders})", row)
                            except Exception as e:
                                logger.error(f"插入数据到表 {table} 失败: {str(e)}")
                        self.target_conn.commit()

                    offset += batch_size
                    logger.info(f"已同步表 {table} 的 {min(offset, total_rows)}/{total_rows} 行数据")
            return True
        except Exception as e:
            logger.error(f"同步表 {table} 数据失败: {str(e)}")
            return False

    def sync_all_tables(self):
        """同步所有表的数据"""
        if not self.connect_databases():
            return False
        try:
            tables = self.get_tables()
            logger.info(f"开始同步, 共 {len(tables)} 个表: {', '.join(tables)}")
            success_count = 0
            for table in tables:
                logger.info(f"正在同步表 {table}")
                # 同步单个表数据
                if self.sync_table_data(table):
                    success_count += 1
            logger.info(f"同步完成, 成功同步 {success_count}/{len(tables)} 个表")
            return success_count == len(tables)
        except Exception as e:
            logger.error(f"同步数据失败: {str(e)}")
            return False
        finally:
            # 关闭数据库连接
            if self.source_conn:
                self.source_conn.close()
            if self.target_conn:
                self.target_conn.close()

    def run_periodic_sync(self):
        """定期运行同步任务"""
        logger.info(f"开始定期同步任务，同步间隔: {self.sync_interval} 秒")
        while True:
            start_time = time.time()
            logger.info(f"开始同步任务 - {datetime.now()}")
            self.sync_all_tables()

            # 计算需要等待的时间
            elapsed = time.time() - start_time
            wait_time = max(0, self.sync_interval - elapsed)

            if wait_time > 0:
                logger.info(f"等待 {wait_time:.2f} 秒进行下一次同步")
                time.sleep(wait_time)


def load_config(config_file):
    """从配置文件加载数据库配置"""
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'source': {
            'host': config.get('source_db', 'host'),
            'port': config.get('source_db', 'port', fallback='3306'),
            'user': config.get('source_db', 'user'),
            'password': config.get('source_db', 'password'),
            'database': config.get('source_db', 'database')
        },
        'target': {
            'host': config.get('target_db', 'host'),
            'port': config.get('target_db', 'port', fallback='3306'),
            'user': config.get('target_db', 'user'),
            'password': config.get('target_db', 'password'),
            'database': config.get('target_db', 'database')
        },
        'sync': {
            'interval': config.getint('sync', 'interval', fallback=60),
            'tables': config.get('sync', 'tables', fallback='').split(',') if config.get('sync', 'tables', fallback='') else None
        }
    }


def create_default_config(config_file):
    """创建默认配置文件"""
    config = configparser.ConfigParser()

    config['source_db'] = {
        'host': '173.162.1.153',
        'port': '3306',
        'user': 'root',
        'password': '12345678',
        'database': 'baozhu'
    }

    config['target_db'] = {
        'host': 'target_host',
        'port': '3306',
        'user': 'root',
        'password': '123456',
        'database': 'test'
    }

    config['sync'] = {
        'interval': '60',
        'tables': 'brand, schedule, tester'  # 留空表示同步所有表，或用逗号分隔的表名列表
    }

    with open(config_file, 'w') as f:
        config.write(f)

    logger.info(f"已创建默认配置文件: {config_file}")


def main():
    parser = argparse.ArgumentParser(description='MySQL数据库同步工具')
    parser.add_argument('-c', '--config', default='db_sync.ini', help='配置文件路径')
    parser.add_argument('--create-config', action='store_true', help='创建默认配置文件')
    parser.add_argument('--once', action='store_true', help='只同步一次，不进行周期性同步')
    args = parser.parse_args()

    # 是否创建默认配置文件
    if args.create_config:
        create_default_config(args.config)
        return

    if not os.path.exists(args.config):
        logger.error(f"配置文件不存在: {args.config}")
        logger.info(f"请使用 --create-config 参数创建默认配置文件")
        return

    # 加载配置文件
    config = load_config(args.config)

    # 创建同步器并运行, 传入配置文件中的源数据库、目标数据库、同步表和同步间隔
    syncer = DatabaseSync(
        config['source'],
        config['target'],
        config['sync']['tables'],
        config['sync']['interval']
    )

    if args.once:
        # 同步一次
        logger.info(f"同步一次")
        syncer.sync_all_tables()
    else:
        # 周期同步
        logger.info(f"=============== 周期性同步 ===============")
        syncer.run_periodic_sync()

if __name__ == "__main__":
    main()