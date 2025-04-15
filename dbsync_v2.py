#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MySQL数据库同步工具 - 增强版
支持增量同步和数据验证功能
"""

import pymysql
import time
import logging
import argparse
import configparser
import os
import hashlib
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("db_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("db_sync")


class DatabaseSync:
    def __init__(self, source_config, target_config, tables=None, sync_interval=60,
                 incremental=True, verify_data=True, last_sync_file="last_sync.json"):
        """
        初始化数据库同步工具

        Args:
            source_config (dict): 源数据库配置
            target_config (dict): 目标数据库配置
            tables (list): 需要同步的表列表，None表示同步所有表
            sync_interval (int): 同步间隔，单位为秒
            incremental (bool): 是否使用增量同步
            verify_data (bool): 是否验证同步后的数据
            last_sync_file (str): 记录上次同步时间的文件
        """
        self.source_config = source_config
        self.target_config = target_config
        self.tables = tables
        self.sync_interval = sync_interval
        self.incremental = incremental
        self.verify_data = verify_data
        self.last_sync_file = last_sync_file
        self.source_conn = None
        self.target_conn = None
        self.last_sync_times = self._load_last_sync_times()

    def _load_last_sync_times(self):
        """加载上次同步时间"""
        if os.path.exists(self.last_sync_file):
            try:
                with open(self.last_sync_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载上次同步时间失败: {str(e)}")
        return {}

    def _save_last_sync_times(self):
        """保存上次同步时间"""
        try:
            with open(self.last_sync_file, 'w') as f:
                json.dump(self.last_sync_times, f)
        except Exception as e:
            logger.error(f"保存上次同步时间失败: {str(e)}")

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
                charset='utf8mb4'
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
            with self.source_conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]
                return tables
        except Exception as e:
            logger.error(f"获取表列表失败: {str(e)}")
            return []

    def get_table_structure(self, table):
        """获取表结构"""
        try:
            with self.source_conn.cursor() as cursor:
                cursor.execute(f"SHOW CREATE TABLE {table}")
                create_statement = cursor.fetchone()[1]
                return create_statement
        except Exception as e:
            logger.error(f"获取表 {table} 结构失败: {str(e)}")
            return None

    def get_table_columns(self, table):
        """获取表的列信息"""
        try:
            with self.source_conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table}")
                columns = [col[0] for col in cursor.fetchall()]
                return columns
        except Exception as e:
            logger.error(f"获取表 {table} 列信息失败: {str(e)}")
            return []

    def get_primary_key(self, table):
        """获取表的主键"""
        try:
            with self.source_conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND CONSTRAINT_NAME = 'PRIMARY'
                    ORDER BY ORDINAL_POSITION
                """, (self.source_config['database'], table))
                primary_keys = [row[0] for row in cursor.fetchall()]
                return primary_keys
        except Exception as e:
            logger.error(f"获取表 {table} 主键失败: {str(e)}")
            return []

    def get_timestamp_columns(self, table):
        """获取表中的时间戳列名（用于增量同步）"""
        timestamp_columns = []
        try:
            with self.source_conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
                    AND (DATA_TYPE LIKE '%time%' OR DATA_TYPE LIKE '%date%')
                    ORDER BY ORDINAL_POSITION
                """, (self.source_config['database'], table))
                timestamp_columns = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取表 {table} 时间戳列失败: {str(e)}")

        return timestamp_columns

    def ensure_table_exists(self, table, create_statement):
        """确保目标数据库中表存在，如果不存在则创建"""
        try:
            with self.target_conn.cursor() as cursor:
                cursor.execute(f"SHOW TABLES LIKE '{table}'")
                if not cursor.fetchone():
                    logger.info(f"在目标数据库中创建表 {table}")
                    cursor.execute(create_statement)
                    self.target_conn.commit()
                return True
        except Exception as e:
            logger.error(f"确保表 {table} 存在失败: {str(e)}")
            return False

    def get_row_count(self, conn, table, condition=None):
        """获取表中的行数"""
        try:
            with conn.cursor() as cursor:
                query = f"SELECT COUNT(*) FROM {table}"
                if condition:
                    query += f" WHERE {condition}"
                cursor.execute(query)
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"获取表 {table} 行数失败: {str(e)}")
            return 0

    def build_incremental_condition(self, table):
        """构建增量同步的条件"""
        last_sync_time = self.last_sync_times.get(table)
        if not last_sync_time or not self.incremental:
            return None

        timestamp_columns = self.get_timestamp_columns(table)
        if not timestamp_columns:
            logger.warning(f"表 {table} 没有时间戳列，无法执行增量同步，将进行全量同步")
            return None

        # 使用第一个时间戳列作为增量同步条件
        update_time_column = timestamp_columns[0]
        return f"`{update_time_column}` > '{last_sync_time}'"

    def calculate_data_checksum(self, conn, table, condition=None, limit=None):
        """计算表数据的校验和"""
        try:
            columns = self.get_table_columns(table)
            if not columns:
                return None

            columns_str = "`,`".join(columns)

            with conn.cursor() as cursor:
                # 构建查询语句
                query = f"SELECT CONCAT(`{columns_str}`) FROM {table}"
                if condition:
                    query += f" WHERE {condition}"
                if limit:
                    query += f" LIMIT {limit}"

                cursor.execute(query)

                # 计算校验和
                hasher = hashlib.md5()
                for row in cursor:
                    # 将所有值转换为字符串并连接
                    row_str = str(row[0])
                    hasher.update(row_str.encode('utf-8'))

                return hasher.hexdigest()
        except Exception as e:
            logger.error(f"计算表 {table} 数据校验和失败: {str(e)}")
            return None

    def verify_table_data(self, table, condition=None, row_limit=1000):
        """验证源表和目标表数据是否一致"""
        # 计算源表数据校验和
        source_checksum = self.calculate_data_checksum(self.source_conn, table, condition, row_limit)
        if not source_checksum:
            return False

        # 计算目标表数据校验和
        target_checksum = self.calculate_data_checksum(self.target_conn, table, condition, row_limit)
        if not target_checksum:
            return False

        # 比较校验和
        is_match = source_checksum == target_checksum
        if is_match:
            logger.info(f"表 {table} 数据验证通过")
        else:
            logger.warning(f"表 {table} 数据验证失败: 源校验和 {source_checksum} ≠ 目标校验和 {target_checksum}")

        return is_match

    def find_different_rows(self, table, primary_keys):
        """查找不同的行数据"""
        if not primary_keys:
            logger.warning(f"表 {table} 没有主键，无法精确比较行差异")
            return

        primary_key_str = "`,`".join(primary_keys)
        columns = self.get_table_columns(table)
        columns_str = "`,`".join(columns)

        try:
            # 查询源表所有数据的主键
            source_keys = {}
            with self.source_conn.cursor() as cursor:
                cursor.execute(f"SELECT `{primary_key_str}`, MD5(CONCAT(`{columns_str}`)) FROM {table}")
                for row in cursor.fetchall():
                    key = tuple(row[:-1])  # 主键值
                    checksum = row[-1]  # 行数据校验和
                    source_keys[key] = checksum

            # 查询目标表所有数据的主键
            target_keys = {}
            with self.target_conn.cursor() as cursor:
                cursor.execute(f"SELECT `{primary_key_str}`, MD5(CONCAT(`{columns_str}`)) FROM {table}")
                for row in cursor.fetchall():
                    key = tuple(row[:-1])  # 主键值
                    checksum = row[-1]  # 行数据校验和
                    target_keys[key] = checksum

            # 找出源表有而目标表没有的行
            missing_keys = set(source_keys.keys()) - set(target_keys.keys())
            if missing_keys:
                logger.warning(f"表 {table} 中有 {len(missing_keys)} 行数据在目标表中缺失")

            # 找出数据内容不同的行
            different_keys = []
            for key in set(source_keys.keys()) & set(target_keys.keys()):
                if source_keys[key] != target_keys[key]:
                    different_keys.append(key)

            if different_keys:
                logger.warning(f"表 {table} 中有 {len(different_keys)} 行数据内容不匹配")

            return missing_keys, different_keys
        except Exception as e:
            logger.error(f"查找表 {table} 行差异失败: {str(e)}")
            return set(), set()

    def sync_table_data(self, table, batch_size=1000):
        """同步单个表的数据"""
        try:
            # 获取表结构并确保目标表存在
            create_statement = self.get_table_structure(table)
            if not create_statement or not self.ensure_table_exists(table, create_statement):
                return False

            # 获取主键信息
            primary_keys = self.get_primary_key(table)
            if not primary_keys:
                logger.warning(f"表 {table} 没有主键，无法执行精确的增量同步或冲突处理")

            # 构建增量同步条件
            condition = self.build_incremental_condition(table)

            # 获取需要同步的行数
            total_rows = self.get_row_count(self.source_conn, table, condition)
            if total_rows == 0:
                logger.info(f"表 {table} 没有需要同步的数据，跳过同步")
                # 仍然更新同步时间
                self.last_sync_times[table] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                return True

            # 获取列信息
            columns = self.get_table_columns(table)
            columns_str = "`,`".join(columns)
            placeholders = ', '.join(['%s'] * len(columns))

            # 获取主键字符串，用于ON DUPLICATE KEY UPDATE语句
            pk_update_stmt = ""
            if primary_keys and self.incremental:
                update_parts = []
                for col in columns:
                    if col not in primary_keys:
                        update_parts.append(f"`{col}` = VALUES(`{col}`)")
                if update_parts:
                    pk_update_stmt = f" ON DUPLICATE KEY UPDATE {', '.join(update_parts)}"

            # 分批同步数据
            offset = 0
            logger.info(f"开始同步表 {table}，共 {total_rows} 行" +
                        (f"（增量同步条件: {condition}）" if condition else "（全量同步）"))

            with self.source_conn.cursor() as source_cursor:
                while offset < total_rows:
                    # 构建查询语句
                    query = f"SELECT `{columns_str}` FROM {table}"
                    if condition:
                        query += f" WHERE {condition}"
                    query += f" LIMIT {offset}, {batch_size}"

                    # 获取当前批次的数据
                    source_cursor.execute(query)
                    rows = source_cursor.fetchall()

                    if not rows:
                        break

                    # 插入数据到目标表
                    with self.target_conn.cursor() as target_cursor:
                        # 构建插入语句
                        insert_query = f"INSERT INTO {table} (`{columns_str}`) VALUES ({placeholders}){pk_update_stmt}"

                        # 批量执行插入
                        target_cursor.executemany(insert_query, rows)
                        self.target_conn.commit()

                    offset += batch_size
                    logger.info(f"已同步表 {table} 的 {min(offset, total_rows)}/{total_rows} 行数据")

            # 更新最后同步时间
            self.last_sync_times[table] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 验证数据
            if self.verify_data:
                logger.info(f"正在验证表 {table} 的数据一致性")
                if not self.verify_table_data(table, condition):
                    # 查找不同的行
                    if primary_keys:
                        missing_keys, different_keys = self.find_different_rows(table, primary_keys)
                        if missing_keys or different_keys:
                            logger.warning(f"表 {table} 数据不一致，尝试修复...")
                            self.repair_inconsistent_data(table, primary_keys, missing_keys, different_keys)

            return True
        except Exception as e:
            logger.error(f"同步表 {table} 数据失败: {str(e)}")
            return False

    def repair_inconsistent_data(self, table, primary_keys, missing_keys, different_keys):
        """修复数据不一致的问题"""
        try:
            columns = self.get_table_columns(table)
            columns_str = "`,`".join(columns)
            placeholders = ', '.join(['%s'] * len(columns))

            # 构建用于WHERE子句的主键条件
            def build_pk_condition(key_values):
                conditions = []
                for i, pk in enumerate(primary_keys):
                    conditions.append(f"`{pk}` = %s")
                return " AND ".join(conditions)

            # 处理缺失的行
            if missing_keys:
                with self.source_conn.cursor() as source_cursor:
                    for key in missing_keys:
                        # 构建条件
                        pk_condition = build_pk_condition(key)
                        # 获取完整行数据
                        query = f"SELECT `{columns_str}` FROM {table} WHERE {pk_condition}"
                        source_cursor.execute(query, key)
                        row = source_cursor.fetchone()

                        if row:
                            # 插入到目标表
                            with self.target_conn.cursor() as target_cursor:
                                insert_query = f"INSERT INTO {table} (`{columns_str}`) VALUES ({placeholders})"
                                target_cursor.execute(insert_query, row)
                                self.target_conn.commit()

                logger.info(f"已修复表 {table} 中 {len(missing_keys)} 行缺失数据")

            # 处理内容不同的行
            if different_keys:
                with self.source_conn.cursor() as source_cursor:
                    for key in different_keys:
                        # 构建条件
                        pk_condition = build_pk_condition(key)
                        # 获取完整行数据
                        query = f"SELECT `{columns_str}` FROM {table} WHERE {pk_condition}"
                        source_cursor.execute(query, key)
                        row = source_cursor.fetchone()

                        if row:
                            # 更新目标表
                            with self.target_conn.cursor() as target_cursor:
                                # 构建SET子句
                                set_parts = []
                                for i, col in enumerate(columns):
                                    if col not in primary_keys:
                                        set_parts.append(f"`{col}` = %s")

                                # 只更新非主键列
                                if set_parts:
                                    non_pk_values = [row[i] for i, col in enumerate(columns) if col not in primary_keys]
                                    update_query = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {pk_condition}"
                                    target_cursor.execute(update_query, non_pk_values + list(key))
                                    self.target_conn.commit()

                logger.info(f"已修复表 {table} 中 {len(different_keys)} 行不一致数据")

        except Exception as e:
            logger.error(f"修复表 {table} 数据不一致失败: {str(e)}")

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
                if self.sync_table_data(table):
                    success_count += 1

            # 保存最后同步时间
            self._save_last_sync_times()

            logger.info(f"同步完成, 成功同步 {success_count}/{len(tables)} 个表")
            return success_count == len(tables)
        except Exception as e:
            logger.error(f"同步数据失败: {str(e)}")
            return False
        finally:
            # 关闭数据库连接
            self.close_connections()

    def close_connections(self):
        """关闭数据库连接"""
        if self.source_conn:
            self.source_conn.close()
            self.source_conn = None
        if self.target_conn:
            self.target_conn.close()
            self.target_conn = None

    def run_periodic_sync(self):
        """定期运行同步任务"""
        logger.info(f"开始定期同步任务，同步间隔: {self.sync_interval} 秒" +
                    (f"，增量同步: {'启用' if self.incremental else '禁用'}" +
                     f"，数据验证: {'启用' if self.verify_data else '禁用'}"))

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
            'tables': config.get('sync', 'tables', fallback='').split(',') if config.get('sync', 'tables',
                                                                                         fallback='') else None,
            'incremental': config.getboolean('sync', 'incremental', fallback=True),
            'verify_data': config.getboolean('sync', 'verify_data', fallback=True)
        }
    }


def create_default_config(config_file):
    """创建默认配置文件"""
    config = configparser.ConfigParser()

    config['source_db'] = {
        'host': 'source_host',
        'port': '3306',
        'user': 'source_user',
        'password': 'source_password',
        'database': 'source_database'
    }

    config['target_db'] = {
        'host': 'target_host',
        'port': '3306',
        'user': 'target_user',
        'password': 'target_password',
        'database': 'target_database'
    }

    config['sync'] = {
        'interval': '60',  # 同步间隔，单位为秒
        'tables': '',  # 留空表示同步所有表，或用逗号分隔的表名列表
        'incremental': 'true',  # 是否使用增量同步
        'verify_data': 'true'  # 是否验证同步后的数据
    }

    with open(config_file, 'w') as f:
        config.write(f)

    logger.info(f"已创建默认配置文件: {config_file}")


def main():
    parser = argparse.ArgumentParser(description='MySQL数据库同步工具')
    parser.add_argument('-c', '--config', default='db_sync.ini', help='配置文件路径')
    parser.add_argument('--create-config', action='store_true', help='创建默认配置文件')
    parser.add_argument('--once', action='store_true', help='只同步一次，不进行周期性同步')
    parser.add_argument('--full', action='store_true', help='执行全量同步，不使用增量同步')
    parser.add_argument('--no-verify', action='store_true', help='不验证同步后的数据')
    args = parser.parse_args()

    if args.create_config:
        create_default_config(args.config)
        return

    if not os.path.exists(args.config):
        logger.error(f"配置文件不存在: {args.config}")
        logger.info(f"使用 --create-config 参数创建默认配置文件")
        return

    # 加载配置
    config = load_config(args.config)

    # 命令行参数可以覆盖配置文件
    incremental = not args.full and config['sync']['incremental']
    verify_data = not args.no_verify and config['sync']['verify_data']

    # 创建同步器并运行
    syncer = DatabaseSync(
        config['source'],
        config['target'],
        config['sync']['tables'],
        config['sync']['interval'],
        incremental=incremental,
        verify_data=verify_data
    )

    if args.once:
        syncer.sync_all_tables()
    else:
        syncer.run_periodic_sync()


if __name__ == "__main__":
    main()