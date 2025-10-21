import re
from datetime import datetime
from pathlib import Path


def parse_filename_time(filename: str) -> datetime:
    """
    从文件名如 2025-09-22-17-31-15-0.mp4 提取时间
    返回 datetime 对象用于排序
    """
    match = re.match(
        r"(\d{4})-(\d{1,2})-(\d{1,2})-(\d{1,2})-(\d{1,2})-(\d{1,2})", filename
    )
    if match:
        year, month, day, hour, minute, second = map(int, match.groups())
        try:
            return datetime(year, month, day, hour, minute, second)
        except ValueError:
            return datetime.min
    return datetime.min


def cleanup_old_videos(path: Path, keep_videos: int):
    """
    扫描 path 下所有 app/stream，保留最新的 keep_videos 个 .mp4 文件，删除旧的。
    若删除后 stream 或 app 目录为空，则也删除该目录。
    """
    print(
        f"[Scheduler {datetime.now()}] 开始扫描 {path} 下所有 app/stream 的视频片段..."
    )

    if not path.exists():
        print(f"[Scheduler Error] ❌ 录像根目录不存在: {path}")
        return

    if not path.is_dir():
        print(f"[Scheduler Error] ❌ 路径不是目录: {path}")
        return

    total_deleted = 0  # 统计总共删除的文件数

    # 遍历每个 app
    for app_path in path.iterdir():
        if not app_path.is_dir():
            continue

        app_deleted_any = False  # 标记这个 app 是否发生了删除操作

        # 遍历每个 stream
        for stream_path in app_path.iterdir():
            if not stream_path.is_dir():
                continue

            try:
                # 获取所有 .mp4 文件（递归查找）
                video_files = list(stream_path.rglob("*.mp4"))

                if len(video_files) <= keep_videos:
                    continue  # 不需要删除

                # 按文件名中的时间排序（新 → 旧）
                sorted_files = sorted(
                    video_files,
                    key=lambda f: parse_filename_time(f.name),
                    reverse=True,
                )

                # 要删除的是：从第 keep_videos 个开始的所有文件
                files_to_delete = sorted_files[keep_videos:]

                stream_deleted_any = False
                for file_path in files_to_delete:
                    try:
                        file_path.unlink()
                        relative_path = file_path.relative_to(path)
                        print(
                            f"[Scheduler {datetime.now()}] 🗑️ 删除旧片段: {relative_path}"
                        )
                        total_deleted += 1
                        stream_deleted_any = True
                    except Exception as e:
                        print(f"[Scheduler Error] ❌ 删除失败 {file_path}: {e}")

                # 如果有删除，并且删除后 stream 目录为空 → 删除目录
                if stream_deleted_any and not any(stream_path.iterdir()):
                    try:
                        stream_path.rmdir()
                        relative_stream = stream_path.relative_to(path)
                        print(
                            f"[Scheduler {datetime.now()}] 📁 删除空 stream 目录: {relative_stream}"
                        )
                        app_deleted_any = True  # 标记 app 层可能也要删
                    except Exception as e:
                        print(
                            f"[Scheduler Error] ❌ 删除 stream 目录失败 {stream_path}: {e}"
                        )
                elif stream_deleted_any:
                    app_deleted_any = True  # stream 还有内容，但至少发生过删除

            except Exception as e:
                print(f"[Scheduler Error] ❌ 处理 stream 失败 {stream_path}: {e}")

        # app 处理结束后：如果 app 下已无任何子项，则删除 app 目录
        if app_deleted_any:
            try:
                if not any(app_path.iterdir()):
                    app_path.rmdir()
                    relative_app = app_path.relative_to(path)
                    print(
                        f"[Scheduler {datetime.now()}] 📂 删除空 app 目录: {relative_app}"
                    )
            except Exception as e:
                print(f"[Scheduler Error] ❌ 删除 app 目录失败 {app_path}: {e}")

    print(
        f"[Scheduler {datetime.now()}] ✅ 扫描与清理完成，共删除 {total_deleted} 个旧视频片段。"
    )
