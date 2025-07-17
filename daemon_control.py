#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HKEX 公告下载器 - 守护者进程控制脚本
作者：Victor Suen
版本：2.1
"""

import sys
import os
import argparse
from main import HKEXDownloaderCLI

def main():
    """守护者进程控制脚本主函数"""
    parser = argparse.ArgumentParser(
        description='HKEX 公告下载器 - 守护者进程控制',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s start                             # 启动守护者进程
  %(prog)s stop                              # 停止守护者进程
  %(prog)s restart                           # 重启守护者进程
  %(prog)s status                            # 查看状态
  %(prog)s test                              # 测试配置
        """)
    
    parser.add_argument('action', 
                       choices=['start', 'stop', 'restart', 'status', 'test'],
                       help='要执行的操作')
    
    parser.add_argument('-c', '--config',
                       default='config.yaml',
                       help='配置文件路径 (默认: config.yaml)')
    
    args = parser.parse_args()
    
    # 构建对应的main.py参数
    main_args = [
        '--config', args.config,
        f'--daemon-{args.action}'
    ]
    
    # 临时修改sys.argv来调用main.py
    original_argv = sys.argv[:]
    try:
        sys.argv = ['main.py'] + main_args
        cli = HKEXDownloaderCLI()
        cli.main()
    finally:
        sys.argv = original_argv

if __name__ == '__main__':
    main()
